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
const CHANNEL_OPEN: [&str; 2] = ["channel_open", "direct_provision"];

fn text<'a>(p: &'a Value, k: &str) -> &'a str { p.get(k).and_then(Value::as_str).unwrap_or("") }

/// Why `tenant` may not run this command, if it may not. Pure, so a test can read every rule.
pub fn refusal(tenant: &str, command: &str, p: &Value) -> Option<String> {
    let c = command;
    if REGISTER.contains(&c) {
        let id = text(p, "id");
        if id != tenant {
            return Some(format!("{} may register only itself, not {:?}: other services register through the operator's manifest", tenant, id));
        }
        if matches!(text(p, "shape"), "node" | "1" | "component" | "2") {
            return Some(format!("{} may not register as a {}", tenant, text(p, "shape")));
        }
        for k in ["constellation", "holds", "peers"] {
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
    } else if CHANNEL_OPEN.contains(&c) {
        return Some(format!("{} may not open a channel: channels are provisioned by the operator", tenant));
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
    let why = refusal(tenant, &command.command, &command.parameters)?;
    warn!("Refused: {}", why);
    Some(CommandResult::error(why))
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
    }

    #[test]
    fn everything_else_passes_untouched() {
        for c in ["ping", "money_sum", "template_fragment", "discover_with_endpoints", "topology_status"] {
            assert!(!refused(c, json!({"id": "site_b"})));
        }
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
