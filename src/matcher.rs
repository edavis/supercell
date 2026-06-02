use anyhow::{anyhow, Context, Result};

use chrono::{DateTime, Utc};
use serde_json_path::JsonPath;

use rhai::{
    serde::to_dynamic, Array, CustomType, Dynamic, Engine, ImmutableString, Scope, TypeBuilder, AST,
};
use regex::Regex;
use std::{
    collections::HashMap,
    path::PathBuf,
    str::FromStr,
    sync::{Mutex, OnceLock},
};

use crate::config;

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, PartialEq)]
pub enum MatchOperation {
    #[default]
    Upsert,
    Update,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, CustomType)]
pub struct Match(pub MatchOperation, pub String);

impl Match {
    fn upsert(aturi: &str) -> Self {
        Self(MatchOperation::Upsert, aturi.to_string())
    }
    fn update(aturi: &str) -> Self {
        Self(MatchOperation::Update, aturi.to_string())
    }
}

pub trait Matcher: Sync + Send {
    fn matches(&self, value: &serde_json::Value) -> Result<Option<Match>>;
}

pub struct FeedMatcher {
    pub(crate) feed: String,
    matchers: Vec<Box<dyn Matcher>>,
}

pub(crate) struct FeedMatchers(pub(crate) Vec<FeedMatcher>);

impl FeedMatchers {
    pub(crate) fn from_config(config_feeds: &config::Feeds) -> Result<Self> {
        let mut feed_matchers = vec![];

        for config_feed in config_feeds.feeds.iter() {
            let feed = config_feed.uri.clone();

            let mut matchers = vec![];

            for config_feed_matcher in config_feed.matchers.iter() {
                match config_feed_matcher {
                    config::Matcher::Equal { path, value, aturi } => {
                        matchers
                            .push(Box::new(EqualsMatcher::new(value, path, aturi)?)
                                as Box<dyn Matcher>);
                    }
                    config::Matcher::Prefix { path, value, aturi } => {
                        matchers
                            .push(Box::new(PrefixMatcher::new(value, path, aturi)?)
                                as Box<dyn Matcher>);
                    }
                    config::Matcher::Sequence {
                        path,
                        values,
                        aturi,
                    } => {
                        matchers.push(Box::new(SequenceMatcher::new(values, path, aturi)?)
                            as Box<dyn Matcher>);
                    }

                    config::Matcher::Rhai { script } => {
                        matchers.push(Box::new(RhaiMatcher::new(script)?) as Box<dyn Matcher>);
                    }
                }
            }

            feed_matchers.push(FeedMatcher { feed, matchers });
        }

        Ok(Self(feed_matchers))
    }
}

impl FeedMatcher {
    pub(crate) fn matches(&self, value: &serde_json::Value) -> Option<Match> {
        for matcher in self.matchers.iter() {
            let result = matcher.matches(value);
            if let Err(err) = result {
                tracing::error!(error = ?err, "matcher returned error");
                continue;
            }
            let result = result.unwrap();
            if result.is_some() {
                return result;
            }
        }
        None
    }
}

pub struct EqualsMatcher {
    expected: String,
    path: JsonPath,
    aturi_path: Option<JsonPath>,
}

impl EqualsMatcher {
    pub fn new(expected: &str, path: &str, aturi: &Option<String>) -> Result<Self> {
        let path = JsonPath::parse(path).context("cannot parse path")?;
        let aturi_path = if let Some(aturi) = aturi {
            let parsed_aturi_path =
                JsonPath::parse(aturi).context("cannot parse aturi jsonpath")?;
            Some(parsed_aturi_path)
        } else {
            None
        };
        Ok(Self {
            expected: expected.to_string(),
            path,
            aturi_path,
        })
    }
}

impl Matcher for EqualsMatcher {
    fn matches(&self, value: &serde_json::Value) -> Result<Option<Match>> {
        let nodes = self.path.query(value).all();

        let string_nodes = nodes
            .iter()
            .filter_map(|value| {
                if let serde_json::Value::String(actual) = value {
                    Some(actual.to_lowercase().clone())
                } else {
                    None
                }
            })
            .collect::<Vec<String>>();

        if string_nodes.iter().any(|value| value == &self.expected) {
            extract_aturi(self.aturi_path.as_ref(), value)
                .map(|value| Some(Match::upsert(&value)))
                .ok_or(anyhow!(
                    "matcher matched but could not create at-uri: {:?}",
                    value
                ))
        } else {
            Ok(None)
        }
    }
}

pub fn matcher_before_duration(duration: &str, date: &str) -> bool {
    let parsed_date = DateTime::parse_from_rfc3339(date).map(|v| v.with_timezone(&Utc));
    if let Err(err) = parsed_date {
        tracing::debug!(error = ?err, "error parsing date");
        return true;
    }
    let (direction, parsed_duration) = if let Some(duration) = duration.strip_prefix('-') {
        (true, duration_str::parse_chrono(duration))
    } else {
        (false, duration_str::parse_chrono(duration))
    };

    if let Err(err) = parsed_duration {
        tracing::debug!(error = ?err, "error parsing date");
        return true;
    }

    let date = parsed_date.unwrap();
    let duration = parsed_duration.unwrap();
    let target = if direction {
        Utc::now() - duration
    } else {
        Utc::now() + duration
    };

    date < target
}

pub struct PrefixMatcher {
    prefix: String,
    path: JsonPath,
    aturi_path: Option<JsonPath>,
}

impl PrefixMatcher {
    pub(crate) fn new(prefix: &str, path: &str, aturi: &Option<String>) -> Result<Self> {
        let path = JsonPath::parse(path).context("cannot parse path")?;
        let aturi_path = if let Some(aturi) = aturi {
            let parsed_aturi_path =
                JsonPath::parse(aturi).context("cannot parse aturi jsonpath")?;
            Some(parsed_aturi_path)
        } else {
            None
        };
        Ok(Self {
            prefix: prefix.to_string(),
            path,
            aturi_path,
        })
    }
}

impl Matcher for PrefixMatcher {
    fn matches(&self, value: &serde_json::Value) -> Result<Option<Match>> {
        let nodes = self.path.query(value).all();

        let string_nodes = nodes
            .iter()
            .filter_map(|value| {
                if let serde_json::Value::String(actual) = value {
                    Some(actual.to_lowercase().clone())
                } else {
                    None
                }
            })
            .collect::<Vec<String>>();

        let found = string_nodes
            .iter()
            .any(|value| value.starts_with(&self.prefix));
        if found {
            extract_aturi(self.aturi_path.as_ref(), value)
                .map(|value| Some(Match::upsert(&value)))
                .ok_or(anyhow!(
                    "matcher matched but could not create at-uri: {:?}",
                    value
                ))
        } else {
            Ok(None)
        }
    }
}

pub struct SequenceMatcher {
    expected: Vec<String>,
    path: JsonPath,
    aturi_path: Option<JsonPath>,
}

impl SequenceMatcher {
    pub(crate) fn new(expected: &[String], path: &str, aturi: &Option<String>) -> Result<Self> {
        let path = JsonPath::parse(path).context("cannot parse path")?;
        let aturi_path = if let Some(aturi) = aturi {
            let parsed_aturi_path =
                JsonPath::parse(aturi).context("cannot parse aturi jsonpath")?;
            Some(parsed_aturi_path)
        } else {
            None
        };
        Ok(Self {
            expected: expected.to_owned(),
            path,
            aturi_path,
        })
    }
}

impl Matcher for SequenceMatcher {
    fn matches(&self, value: &serde_json::Value) -> Result<Option<Match>> {
        let nodes = self.path.query(value).all();

        let string_nodes = nodes
            .iter()
            .filter_map(|value| {
                if let serde_json::Value::String(actual) = value {
                    Some(actual.to_lowercase().clone())
                } else {
                    None
                }
            })
            .collect::<Vec<String>>();

        for string_node in string_nodes {
            let mut last_found: i32 = -1;

            let mut found_index = 0;
            for (index, expected) in self.expected.iter().enumerate() {
                if let Some(current_found) = string_node.find(expected) {
                    if (current_found as i32) > last_found {
                        last_found = current_found as i32;
                        found_index = index;
                    } else {
                        last_found = -1;
                        break;
                    }
                } else {
                    last_found = -1;
                    break;
                }
            }

            if last_found != -1 && found_index == self.expected.len() - 1 {
                return extract_aturi(self.aturi_path.as_ref(), value)
                    .map(|value| Some(Match::upsert(&value)))
                    .ok_or(anyhow!(
                        "matcher matched but could not create at-uri: {:?}",
                        value
                    ));
            }
        }

        Ok(None)
    }
}

pub fn matcher_sequence_matches(sequence: Array, text: ImmutableString) -> bool {
    let sequence = sequence
        .iter()
        .filter_map(|value| value.clone().try_cast::<String>())
        .collect::<Vec<String>>();
    sequence_matches(sequence.as_ref(), &text)
}

pub fn matcher_matches(text: ImmutableString, pattern: ImmutableString) -> bool {
    static CACHE: OnceLock<Mutex<HashMap<String, Regex>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));

    let mut cache = match cache.lock() {
        Ok(cache) => cache,
        Err(err) => {
            tracing::warn!(error = ?err, "regex cache lock poisoned");
            return false;
        }
    };

    if !cache.contains_key(pattern.as_str()) {
        match Regex::new(&pattern) {
            Ok(regex) => {
                cache.insert(pattern.to_string(), regex);
            }
            Err(err) => {
                tracing::warn!(error = ?err, pattern = %pattern, "error compiling regex");
                return false;
            }
        }
    }

    cache[pattern.as_str()].is_match(&text)
}

fn sequence_matches(sequence: &[String], text: &str) -> bool {
    let mut last_found: i32 = -1;

    let mut found_index = 0;
    for (index, expected) in sequence.iter().enumerate() {
        if let Some(current_found) = text.find(expected) {
            if (current_found as i32) > last_found {
                last_found = current_found as i32;
                found_index = index;
            } else {
                last_found = -1;
                break;
            }
        } else {
            last_found = -1;
            break;
        }
    }
    last_found != -1 && found_index == sequence.len() - 1
}

fn extract_aturi(aturi: Option<&JsonPath>, event_value: &serde_json::Value) -> Option<String> {
    if let Some(aturi_path) = aturi {
        let nodes = aturi_path.query(event_value).all();
        let string_nodes = nodes
            .iter()
            .filter_map(|value| {
                if let serde_json::Value::String(actual) = value {
                    Some(actual.to_lowercase().clone())
                } else {
                    None
                }
            })
            .collect::<Vec<String>>();

        for value in string_nodes {
            if value.starts_with("at://") {
                return Some(value);
            }
        }
    }

    let rtype = event_value
        .get("commit")
        .and_then(|commit| commit.get("record"))
        .and_then(|commit| commit.get("$type"))
        .and_then(|did| did.as_str());

    if Some("app.bsky.feed.post") == rtype {
        let did = event_value.get("did").and_then(|did| did.as_str())?;
        let commit = event_value.get("commit")?;
        let collection = commit.get("collection").and_then(|did| did.as_str())?;
        let rkey = commit.get("rkey").and_then(|did| did.as_str())?;
        let uri = format!("at://{}/{}/{}", did, collection, rkey);
        return Some(uri);
    }

    if Some("app.bsky.feed.like") == rtype {
        return event_value
            .get("commit")
            .and_then(|value| value.get("record"))
            .and_then(|value| value.get("subject"))
            .and_then(|value| value.get("uri"))
            .and_then(|value| value.as_str())
            .map(|value| value.to_string());
    }

    None
}

pub struct RhaiMatcher {
    source: String,
    engine: Engine,
    ast: AST,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum MaybeMatch {
    Match(Match),

    #[serde(untagged)]
    Other {
        #[serde(flatten)]
        extra: HashMap<String, serde_json::Value>,
    },
}

impl MaybeMatch {
    pub fn into_match(self) -> Option<Match> {
        match self {
            MaybeMatch::Match(m) => Some(m),
            _ => None,
        }
    }
}

impl RhaiMatcher {
    pub fn new(source: &str) -> Result<Self> {
        let mut engine = Engine::new();
        engine
            .build_type::<Match>()
            .register_fn("build_aturi", build_aturi)
            .register_fn("sequence_matches", matcher_sequence_matches)
            .register_fn("matches", matcher_matches)
            .register_fn("matcher_before_duration", matcher_before_duration)
            .register_fn("update_match", Match::update)
            .register_fn("upsert_match", Match::upsert);
        let ast = engine
            .compile_file(PathBuf::from_str(source)?)
            .context("cannot compile script")?;
        Ok(Self {
            source: source.to_string(),
            engine,
            ast,
        })
    }
}

fn dynamic_to_match(value: Dynamic) -> Result<Option<Match>> {
    if value.is_bool() || value.is_int() {
        return Ok(None);
    }
    if let Some(aturi) = value.clone().try_cast::<String>() {
        return Ok(Some(Match::upsert(&aturi)));
    }
    if let Some(match_value) = value.try_cast::<Match>() {
        return Ok(Some(match_value));
    }
    Err(anyhow!(
        "unsupported return value type: must be int, string, or match"
    ))
}

impl Matcher for RhaiMatcher {
    fn matches(&self, value: &serde_json::Value) -> Result<Option<Match>> {
        let mut scope = Scope::new();
        let value_map = to_dynamic(value);
        if let Err(err) = value_map {
            tracing::error!(source = ?self.source, error = ?err, "error converting value to dynamic");
            return Ok(None);
        }
        let value_map = value_map.unwrap();
        scope.push("event", value_map);

        self.engine
            .eval_ast_with_scope::<Dynamic>(&mut scope, &self.ast)
            .context("error evaluating script")
            .and_then(dynamic_to_match)
    }
}

fn build_aturi_maybe(event: Dynamic) -> Result<String> {
    let event = event.as_map_ref().map_err(|err| anyhow!(err))?;

    let commit = event
        .get("commit")
        .ok_or(anyhow!("no commit on event"))?
        .as_map_ref()
        .map_err(|err| anyhow!(err))?;
    let record = commit
        .get("record")
        .ok_or(anyhow!("no record on event commit"))?
        .as_map_ref()
        .map_err(|err| anyhow!(err))?;

    let rtype = record
        .get("$type")
        .ok_or(anyhow!("no $type on event commit record"))?
        .as_immutable_string_ref()
        .map_err(|err| anyhow!(err))?;

    match rtype.as_str() {
        "app.bsky.feed.post" => {
            let did = event
                .get("did")
                .ok_or(anyhow!("no did on event"))?
                .as_immutable_string_ref()
                .map_err(|err| anyhow!(err))?;
            let collection = commit
                .get("collection")
                .ok_or(anyhow!("no collection on event"))?
                .as_immutable_string_ref()
                .map_err(|err| anyhow!(err))?;
            let rkey = commit
                .get("rkey")
                .ok_or(anyhow!("no rkey on event commit"))?
                .as_immutable_string_ref()
                .map_err(|err| anyhow!(err))?;

            Ok(format!(
                "at://{}/{}/{}",
                did.as_str(),
                collection.as_str(),
                rkey.as_str()
            ))
        }
        "app.bsky.feed.like" => {
            let subject = record
                .get("subject")
                .ok_or(anyhow!("no subject on event commit record"))?
                .as_map_ref()
                .map_err(|err| anyhow!(err))?;
            let uri = subject
                .get("uri")
                .ok_or(anyhow!("no uri on event commit record subject"))?
                .as_immutable_string_ref()
                .map_err(|err| anyhow!(err))?;

            Ok(uri.to_string())
        }

        _ => Err(anyhow!("no aturi for event")),
    }
}

fn build_aturi(event: Dynamic) -> String {
    let aturi = build_aturi_maybe(event);
    if let Err(err) = aturi {
        tracing::warn!(error = ?err, "error creating at-uri");
        return "".into();
    }
    aturi.unwrap()
}

#[cfg(test)]
mod tests {

    use super::*;
    use anyhow::{anyhow, Result};
    use std::path::PathBuf;

    #[test]
    fn equals_matcher() -> Result<()> {
        let raw_json = r#"{
    "did": "did:plc:tgudj2fjm77pzkuawquqhsxm",
    "time_us": 1730491093829414,
    "kind": "commit",
    "commit": {
        "rev": "3l7vxhiuibq2u",
        "operation": "create",
        "collection": "app.bsky.feed.post",
        "rkey": "3l7vxhiu4kq2u",
        "record": {
            "$type": "app.bsky.feed.post",
            "createdAt": "2024-11-01T19:58:12.980Z",
            "langs": ["en", "es"],
            "text": "hey dnd question, what does a 45 on a stealth check look like"
        },
        "cid": "bafyreide7jpu67vvkn4p2iznph6frbwv6vamt7yg5duppqjqggz4sdfik4"
    }
}"#;

        let value: serde_json::Value = serde_json::from_str(raw_json).expect("json is valid");

        let tests = vec![
            ("$.did", "did:plc:tgudj2fjm77pzkuawquqhsxm", true),
            ("$.commit.record['$type']", "app.bsky.feed.post", true),
            ("$.commit.record.langs.*", "en", true),
            (
                "$.commit.record.text",
                "hey dnd question, what does a 45 on a stealth check look like",
                true,
            ),
            ("$.did", "did:plc:tgudj2fjm77pzkuawquqhsxn", false),
            ("$.commit.record.notreal", "value", false),
        ];

        for (path, expected, result) in tests {
            let matcher = EqualsMatcher::new(expected, path, &None).expect("matcher is valid");
            let maybe_match = matcher.matches(&value)?;
            assert_eq!(maybe_match.is_some(), result);
        }

        Ok(())
    }

    #[test]
    fn prefix_matcher() -> Result<()> {
        let raw_json = r#"{
    "did": "did:plc:tgudj2fjm77pzkuawquqhsxm",
    "time_us": 1730491093829414,
    "kind": "commit",
    "commit": {
        "rev": "3l7vxhiuibq2u",
        "operation": "create",
        "collection": "app.bsky.feed.post",
        "rkey": "3l7vxhiu4kq2u",
        "record": {
            "$type": "app.bsky.feed.post",
            "createdAt": "2024-11-01T19:58:12.980Z",
            "langs": ["en"],
            "text": "hey dnd question, what does a 45 on a stealth check look like",
            "facets": [
                {
                    "features": [{"$type": "app.bsky.richtext.facet#tag", "tag": "dungeonsanddragons"}],
                    "index": { "byteEnd": 1, "byteStart": 0 }
                },
                {
                    "features": [{"$type": "app.bsky.richtext.facet#tag", "tag": "gaming"}],
                    "index": { "byteEnd": 1, "byteStart": 0 }
                }
            ]
        },
        "cid": "bafyreide7jpu67vvkn4p2iznph6frbwv6vamt7yg5duppqjqggz4sdfik4"
    }
}"#;

        let value: serde_json::Value = serde_json::from_str(raw_json).expect("json is valid");

        let tests = vec![
            ("$.commit.record['$type']", "app.bsky.", true),
            ("$.commit.record.langs.*", "e", true),
            ("$.commit.record.text", "hey dnd question", true),
            ("$.commit.record.facets[*].features[?(@['$type'] == 'app.bsky.richtext.facet#tag')].tag", "dungeons", true),
            ("$.commit.record.notreal", "value", false),
            ("$.commit.record['$type']", "com.bsky.", false),
        ];

        for (path, prefix, result) in tests {
            let matcher = PrefixMatcher::new(prefix, path, &None).expect("matcher is valid");
            let maybe_match = matcher.matches(&value)?;
            assert_eq!(maybe_match.is_some(), result);
        }

        Ok(())
    }

    #[test]
    fn sequence_matcher() -> Result<()> {
        let raw_json = r#"{
    "did": "did:plc:tgudj2fjm77pzkuawquqhsxm",
    "time_us": 1730491093829414,
    "kind": "commit",
    "commit": {
        "rev": "3l7vxhiuibq2u",
        "operation": "create",
        "collection": "app.bsky.feed.post",
        "rkey": "3l7vxhiu4kq2u",
        "record": {
            "$type": "app.bsky.feed.post",
            "createdAt": "2024-11-01T19:58:12.980Z",
            "langs": ["en"],
            "text": "hey dnd question, what does a 45 on a stealth check look like",
            "facets": [
                {
                    "features": [{"$type": "app.bsky.richtext.facet#tag", "tag": "dungeonsanddragons"}],
                    "index": { "byteEnd": 1, "byteStart": 0 }
                },
                {
                    "features": [{"$type": "app.bsky.richtext.facet#tag", "tag": "gaming"}],
                    "index": { "byteEnd": 1, "byteStart": 0 }
                }
            ]
        },
        "cid": "bafyreide7jpu67vvkn4p2iznph6frbwv6vamt7yg5duppqjqggz4sdfik4"
    }
}"#;

        let value: serde_json::Value = serde_json::from_str(raw_json).expect("json is valid");

        let tests = vec![
            (
                "$.commit.record.text",
                vec!["hey".into(), "dnd".into(), "question".into()],
                true,
            ),
            (
                "$.commit.record.facets[*].features[?(@['$type'] == 'app.bsky.richtext.facet#tag')].tag",
                vec!["dungeons".into(), "and".into(), "dragons".into()],
                true,
            ),
            (
                "$.commit.record.text",
                vec!["hey".into(), "question".into(), "dnd".into()],
                false,
            ),
            (
                "$.commit.record.operation",
                vec!["hey".into(), "dnd".into(), "question".into()],
                false,
            ),
            (
                "$.commit.record.text",
                vec!["hey".into(), "nick".into()],
                false,
            ),
        ];

        for (path, values, result) in tests {
            let matcher = SequenceMatcher::new(&values, path, &None).expect("matcher is valid");
            let maybe_match = matcher.matches(&value)?;
            assert_eq!(maybe_match.is_some(), result);
        }

        Ok(())
    }

    #[test]
    fn sequence_matcher_edge_case_1() -> Result<()> {
        let raw_json = r#"{"text": "Stellwerkstörung. Und Signalstörung.  Und der Alternativzug ist auch ausgefallen. Und überhaupt."}"#;
        let value: serde_json::Value = serde_json::from_str(raw_json).expect("json is valid");
        let matcher = SequenceMatcher::new(
            &vec!["smoke".to_string(), "signal".to_string()],
            "$.text",
            &None,
        )?;
        let maybe_match = matcher.matches(&value)?;
        assert_eq!(maybe_match.is_some(), false);

        Ok(())
    }

    #[test]
    fn rhai_matcher() -> Result<()> {
        let testdata = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("testdata");

        let tests: Vec<(&str, Vec<(&str, bool, &str)>)> = vec![
            (
                "post1.json",
                vec![
                    ("rhai_match_nothing.rhai", false, ""),
                    (
                        "rhai_match_everything.rhai",
                        true,
                        "at://did:plc:cbkjy5n7bk3ax2wplmtjofq2/app.bsky.feed.post/3laadb7behk25",
                    ),
                    (
                        "rhai_match_everything_simple.rhai",
                        true,
                        "at://did:plc:cbkjy5n7bk3ax2wplmtjofq2/app.bsky.feed.post/3laadb7behk25",
                    ),
                    (
                        "rhai_match_type.rhai",
                        true,
                        "at://did:plc:cbkjy5n7bk3ax2wplmtjofq2/app.bsky.feed.post/3laadb7behk25",
                    ),
                    (
                        "rhai_match_poster.rhai",
                        true,
                        "at://did:plc:cbkjy5n7bk3ax2wplmtjofq2/app.bsky.feed.post/3laadb7behk25",
                    ),
                    ("rhai_match_reply_root.rhai", false, ""),
                    (
                        "rhai_match_sequence.rhai",
                        true,
                        "at://did:plc:cbkjy5n7bk3ax2wplmtjofq2/app.bsky.feed.post/3laadb7behk25",
                    ),
                    (
                        "rhai_match_links.rhai",
                        true,
                        "at://did:plc:cbkjy5n7bk3ax2wplmtjofq2/app.bsky.feed.post/3laadb7behk25",
                    ),
                ],
            ),
            (
                "post2.json",
                vec![
                    (
                        "rhai_match_everything.rhai",
                        true,
                        "at://did:plc:cbkjy5n7bk3ax2wplmtjofq2/app.bsky.feed.post/3laadftr72k25",
                    ),
                    (
                        "rhai_match_type.rhai",
                        true,
                        "at://did:plc:cbkjy5n7bk3ax2wplmtjofq2/app.bsky.feed.post/3laadftr72k25",
                    ),
                    (
                        "rhai_match_poster.rhai",
                        true,
                        "at://did:plc:cbkjy5n7bk3ax2wplmtjofq2/app.bsky.feed.post/3laadftr72k25",
                    ),
                    (
                        "rhai_match_reply_root.rhai",
                        true,
                        "at://did:plc:cbkjy5n7bk3ax2wplmtjofq2/app.bsky.feed.post/3laadftr72k25",
                    ),
                ],
            ),
        ];

        for (input_json, matcher_tests) in tests {
            let input_json_path = testdata.join(input_json);
            let json_content = std::fs::read(input_json_path).map_err(|err| {
                anyhow::Error::new(err).context(anyhow!("reading input_json failed"))
            })?;
            let value: serde_json::Value =
                serde_json::from_slice(&json_content).context("parsing input_json failed")?;

            for (matcher_file_name, matched, aturi) in matcher_tests {
                let matcher_path = testdata.join(matcher_file_name);
                let matcher = RhaiMatcher::new(&matcher_path.to_string_lossy())
                    .context("could not construct matcher")?;
                let result = matcher.matches(&value)?;
                assert_eq!(
                    result.is_some_and(|e| e.1 == aturi),
                    matched,
                    "matched {}: {}",
                    input_json,
                    matcher_file_name
                );
            }
        }

        Ok(())
    }

    #[test]
    fn matcher_before_duration() {
        assert!(super::matcher_before_duration(
            "1mon",
            "2024-11-15T11:05:01.000Z"
        ));
        assert!(!super::matcher_before_duration(
            "-1mon",
            "2024-11-15T11:05:01.000Z"
        ));
    }
}
