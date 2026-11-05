//! Who a command is from, and what a tenant may do to the shared topology.
//!
//! The caller is the stream a command was read from: a site's credential can write only its own
//! tag, while `ss` is whatever the caller wrote. A served constellation's own tag belongs to the
//! daemon and the operator's tools (clients only read it), and is not limited here.

use crate::daemon::Command;
use crate::integration::handlers::types::CommandResult;
use log::warn;
use serde_json::Value;

const REGISTER: [&str; 3] = ["register_service", "registerService", "REGISTER_SERVICE"];
const DEREGISTER: [&str; 3] = ["deregister_service", "deregisterService", "DEREGISTER_SERVICE"];
const EDGE_WRITE: [&str; 2] = ["topology_edge_add", "topology_edge_remove"];
const CHANNEL_OPERATOR: [&str; 6] = ["channel_open", "direct_provision", "channel_info", "channel_list", "direct_info", "direct_list"];
const STREAM_READ: [&str; 8] = ["stream_info", "STREAM_INFO", "stream_group_info", "STREAM_GROUP_INFO", "stream_consumer_info", "STREAM_CONSUMER_INFO", "stream_pending", "STREAM_PENDING"];
const DISCOVER_ENDPOINTS: [&str; 3] = ["discover_with_endpoints", "DISCOVER_WITH_ENDPOINTS", "service_endpoints"];

fn text<'a>(p: &'a Value, k: &str) -> &'a str { p.get(k).and_then(Value::as_str).unwrap_or("") }

/// Why `tenant` may not run this command, if it may not. Pure, so a test can read every rule.
pub fn refusal(tenant: &str, command: &str, p: &Value) -> Option<String> {
    let c = command;
    if REGISTER.contains(&c) {
        let id = text(p, "id");
        if id != tenant {
            return Some(format!("{} may register only itself, not {:?}: other services register through the operator's manifest", tenant, id));
        }
        // A provider or a placement receives other tenants' relayed work: those come from the manifest.
        if !matches!(text(p, "shape"), "" | "site" | "web" | "3.1" | "app" | "headless" | "3.2") {
            return Some(format!("{} may not register as a {}", tenant, text(p, "shape")));
        }
        for k in ["constellation", "holds", "peers", "on"] {
            if p.get(k).is_some_and(|v| !v.is_null() && v != &Value::Array(vec![])) {
                return Some(format!("{} may not declare {}: routing facts come from the operator's manifest", tenant, k));
            }
        }
    } else if DEREGISTER.contains(&c) {
        let id = p.get("service_id").or_else(|| p.get("id")).and_then(Value::as_str).unwrap_or("");
        if id != tenant {
            return Some(format!("{} may deregister only itself, not {:?}", tenant, id));
        }
    } else if EDGE_WRITE.contains(&c) {
        if text(p, "from") != tenant {
            return Some(format!("{} may change only its own edges, not those of {:?}", tenant, text(p, "from")));
        }
    } else if CHANNEL_OPERATOR.contains(&c) {
        return Some(format!("{} may not open or list channels: channels are provisioned by the operator", tenant));
    } else if STREAM_READ.contains(&c) {
        let stream = text(p, "stream");
        if crate::config::site_of_stream_key(stream) != Some(tenant) {
            return Some(format!("{} may read only its own streams, not {:?}", tenant, stream));
        }
    } else if DISCOVER_ENDPOINTS.contains(&c) {
        if let Some(reg) = p.get("endpoint_registry").and_then(Value::as_str) {
            if crate::config::site_of_stream_key(reg) != Some(tenant) {
                return Some(format!("{} may read only its own endpoint registry, not {:?}", tenant, reg));
            }
        }
    } else if c.starts_with("topo_") || c.starts_with("registry_") || c.starts_with("cross_") {
        return Some(format!("{} may not use {}: named topologies are shared, and the operator's", tenant, c));
    }
    None
}

/// The tenant a stream belongs to; None for a served constellation's own stream or an untagged one.
pub fn tenant_of_stream(stream_key: &str) -> Option<&str> {
    let tag = crate::config::site_of_stream_key(stream_key).filter(|t| !t.is_empty())?;
    if crate::constellation::served().iter().any(|c| c.name == tag) {
        return None;
    }
    Some(tag)
}

/// Name the caller by its stream and apply the rules. The refusal, if any, is the command's answer.
pub fn apply(stream_key: &str, command: &mut Command) -> Option<CommandResult> {
    let tenant = tenant_of_stream(stream_key)?;
    if command.site_id != tenant {
        if !command.site_id.is_empty() {
            warn!("Command {} on {}'s stream names site {:?}: answered as {}", command.command, tenant, command.site_id, tenant);
        }
        command.site_id = tenant.to_string();
    }
    if let Some(why) = refusal(tenant, &command.command, &command.parameters) {
        warn!("Refused: {}", why);
        return Some(CommandResult::error(why));
    }
    own_templates(tenant, command);
    None
}

const TEMPLATE_COMMANDS: [&str; 4] = ["template_fragment", "TEMPLATE_FRAGMENT", "render_template", "RENDER_TEMPLATE"];

/// One template engine serves every site: a site's templates carry its name there, includes too,
/// so a site neither overwrites nor renders another's.
fn own_templates(tenant: &str, command: &mut Command) {
    if !TEMPLATE_COMMANDS.contains(&command.command.as_str()) {
        return;
    }
    let Some(p) = command.parameters.as_object_mut() else { return };
    if let Some(id) = p.get("template_id").and_then(Value::as_str).map(str::to_string) {
        p.insert("template_id".into(), Value::String(owned_template_name(tenant, &id)));
    }
    if let Some(content) = p.get("content").and_then(Value::as_str).map(str::to_string) {
        p.insert("content".into(), Value::String(own_includes(tenant, &content)));
    }
}

fn owned_template_name(tenant: &str, id: &str) -> String {
    format!("{}__{}", tenant, id)
}

fn own_includes(tenant: &str, content: &str) -> String {
    static NAMED: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let named = NAMED.get_or_init(|| regex::Regex::new(r#"(\{%-?\s*(?:include|extends|import)\s+)(["'])([^"']+)["']"#).expect("a fixed pattern"));
    named.replace_all(content, |c: &regex::Captures| format!("{}{}{}{}", &c[1], &c[2], owned_template_name(tenant, &c[3]), &c[2])).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn refused(c: &str, p: Value) -> bool { refusal("site_a", c, &p).is_some() }

    #[test]
    fn a_tenant_registers_only_itself_and_never_routing_facts() {
        assert!(!refused("register_service", json!({"id": "site_a", "shape": "site", "axes": {}})));
        assert!(!refused("register_service", json!({"id": "site_a", "holds": [], "constellation": null})));
        for alias in REGISTER {
            assert!(refused(alias, json!({"id": "site_b"})), "{alias} took another id");
        }
        assert!(refused("register_service", json!({})));
        for shape in ["node", "1", "component", "2"] {
            assert!(refused("register_service", json!({"id": "site_a", "shape": shape})));
        }
        for shape in ["daemon", "service", "3.3"] {
            assert!(refused("register_service", json!({"id": "site_a", "shape": shape})), "a tenant as a provider receives others' relayed work");
        }
        assert!(refused("register_service", json!({"id": "site_a", "shape": "site", "on": "aesir"})));
        for extra in [json!({"holds": ["x"]}), json!({"peers": ["x"]}), json!({"constellation": "coursematch"})] {
            let mut p = json!({"id": "site_a"});
            p.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
            assert!(refused("register_service", p));
        }
    }

    #[test]
    fn a_tenant_deregisters_only_itself() {
        assert!(!refused("deregister_service", json!({"service_id": "site_a"})));
        assert!(!refused("deregister_service", json!({"id": "site_a"})));
        for alias in DEREGISTER {
            assert!(refused(alias, json!({"service_id": "site_b"})));
            assert!(refused(alias, json!({"id": "aesir-solutions"})));
        }
        assert!(refused("deregister_service", json!({})));
    }

    #[test]
    fn edges_and_channels() {
        assert!(!refused("topology_edge_add", json!({"from": "site_a", "to": "gCore"})));
        assert!(refused("topology_edge_add", json!({"from": "gCore", "to": "site_a"})));
        assert!(refused("topology_edge_remove", json!({"from": "site_b", "to": "gCore"})));
        assert!(!refused("topology_edge_list", json!({"id": "site_b"})));
        assert!(refused("channel_open", json!({"target_site": "site_b"})));
        assert!(refused("direct_provision", json!({"target_site": "site_b"})));
        assert!(refused("channel_list", json!({})));
    }

    #[test]
    fn a_tenant_reads_only_its_own_streams_and_no_shared_topology() {
        assert!(!refused("stream_info", json!({"stream": "{site_a}:gnode:unified:production"})));
        for s in ["{site_b}:gnode:unified:production", "{geodineum}:gnode:health", ""] {
            assert!(refused("stream_pending", json!({"stream": s})), "{s}");
        }
        assert!(refused("stream_info", json!({})));
        assert!(!refused("discover_with_endpoints", json!({"endpoint_registry": "{site_a}:gnode:endpoints"})));
        assert!(refused("discover_with_endpoints", json!({"endpoint_registry": "{site_b}:gnode:endpoints"})));
        for c in ["topo_create", "topo_delete", "topo_map_entity", "registry_get", "cross_union"] {
            assert!(refused(c, json!({"name": "x"})));
        }
    }

    #[test]
    fn everything_else_passes_untouched() {
        for c in ["ping", "money_sum", "template_fragment", "discover_with_endpoints", "topology_status"] {
            assert!(!refused(c, json!({"id": "site_b"})));
        }
    }

    #[test]
    fn a_sites_templates_carry_its_name_includes_too() {
        let mut c = Command { id: "1".into(), command: "template_fragment".into(),
            parameters: json!({"template_id": "home", "content": r#"{% include "head" %}{%- extends 'base' %}{{ x }}"#}),
            site_id: "site_a".into(), node_id: "n".into(), timestamp: 0.0 };
        assert!(apply("{site_a}:gnode:unified:production", &mut c).is_none());
        assert_eq!(c.parameters["template_id"], "site_a__home");
        assert_eq!(c.parameters["content"], r#"{% include "site_a__head" %}{%- extends 'site_a__base' %}{{ x }}"#);
        let mut r = Command { command: "render_template".into(), parameters: json!({"template_id": "home"}), ..c.clone() };
        assert!(apply("{site_b}:gnode:unified:production", &mut r).is_none());
        assert_eq!(r.parameters["template_id"], "site_b__home");
    }

    #[test]
    fn the_caller_is_the_stream_not_what_it_wrote() {
        let mut c = Command { id: "1".into(), command: "deregister_service".into(), parameters: json!({"service_id": "site_b"}),
            site_id: "site_b".into(), node_id: "n".into(), timestamp: 0.0 };
        assert!(apply("{site_a}:gnode:unified:production", &mut c).is_some(), "writing ss = the victim must not pass");
        assert_eq!(c.site_id, "site_a");
        let mut own = Command { parameters: json!({"service_id": "site_a"}), ..c.clone() };
        assert!(apply("{site_a}:gnode:unified:production", &mut own).is_none());
        assert_eq!(tenant_of_stream("no-tag"), None);
    }
}
