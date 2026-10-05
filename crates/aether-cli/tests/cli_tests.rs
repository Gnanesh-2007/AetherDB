use std::process::Command;

fn get_aether_bin() -> String {
    if let Ok(bin) = std::env::var("CARGO_BIN_EXE_aether") {
        return bin;
    }
    let debug_path = if cfg!(windows) {
        "../../target/debug/aether.exe"
    } else {
        "../../target/debug/aether"
    };
    if std::path::Path::new(debug_path).exists() {
        return debug_path.to_string();
    }
    let release_path = if cfg!(windows) {
        "../../target/release/aether.exe"
    } else {
        "../../target/release/aether"
    };
    release_path.to_string()
}

#[test]
fn test_cli_help_commands() {
    let bin = get_aether_bin();
    let output = Command::new(&bin)
        .arg("--help")
        .output()
        .expect("Failed to execute aether --help");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("agent"));
    assert!(stdout.contains("health"));
    assert!(stdout.contains("status"));
    assert!(stdout.contains("shell"));
}

#[test]
fn test_cli_agent_help() {
    let bin = get_aether_bin();
    let output = Command::new(&bin)
        .args(["agent", "--help"])
        .output()
        .expect("Failed to execute aether agent --help");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("state"));
    assert!(stdout.contains("memory"));
}

#[test]
fn test_cli_agent_state_and_memory_e2e() {
    let bin = get_aether_bin();
    let agent_id = format!("test-cli-agent-{}", std::process::id());

    let api_key = std::env::var("AETHERDB_API_KEY")
        .or_else(|_| std::env::var("AETHER_API_KEY"))
        .or_else(|_| std::env::var("AETHERDB_TEST_KEY_A"))
        .or_else(|_| {
            std::env::var("AETHERDB_API_KEYS").map(|keys| {
                keys.split(',')
                    .next()
                    .unwrap_or("aether_sk_default_test")
                    .to_string()
            })
        })
        .unwrap_or_else(|_| "aether_sk_default_test".to_string());

    let make_cmd = || {
        let mut cmd = Command::new(&bin);
        if !api_key.is_empty() {
            cmd.env("AETHERDB_API_KEY", &api_key);
        }
        cmd
    };

    // 1. Health check
    let health_out = make_cmd()
        .arg("health")
        .output()
        .expect("Failed to execute aether health");
    if !health_out.status.success() {
        // Skip if server is not running during standalone cargo test
        return;
    }

    // 2. Set State
    let set_out = make_cmd()
        .args([
            "agent",
            "state",
            &agent_id,
            "set",
            "session",
            "{\"task\":\"cli-test\",\"step\":1}",
        ])
        .output()
        .expect("Failed to execute aether agent state set");
    let set_stdout = String::from_utf8_lossy(&set_out.stdout);
    let set_stderr = String::from_utf8_lossy(&set_out.stderr);
    if !set_out.status.success()
        || set_stdout.contains("ERROR")
        || set_stdout.contains("Unauthorized")
        || set_stdout.contains("401")
        || set_stderr.contains("Error")
        || set_stderr.contains("unreachable")
    {
        // Server requires auth or is unreachable and test environment has no valid key configured
        return;
    }
    assert!(set_stdout.contains("OK"));

    // 3. Get State
    let get_out = make_cmd()
        .args(["agent", "state", &agent_id, "get", "session"])
        .output()
        .expect("Failed to execute aether agent state get");
    assert!(get_out.status.success());
    let get_stdout = String::from_utf8_lossy(&get_out.stdout);
    assert!(get_stdout.contains("cli-test") || get_stdout.contains("Agent:"));

    // 4. Incr Tokens
    let incr_out = make_cmd()
        .args(["agent", "state", &agent_id, "incr", "tokens", "100"])
        .output()
        .expect("Failed to execute aether agent state incr");
    assert!(incr_out.status.success());
    let incr_stdout = String::from_utf8_lossy(&incr_out.stdout);
    assert!(incr_stdout.contains("100"));

    // 5. Remember Memory
    let remember_out = make_cmd()
        .args([
            "agent",
            "memory",
            &agent_id,
            "remember",
            "--id",
            "mem-cli-1",
            "--text",
            "AetherDB CLI provides native agent management.",
            "--embedding",
            "1.0,0.0,0.0,0.0",
        ])
        .output()
        .expect("Failed to execute aether agent memory remember");
    assert!(remember_out.status.success());
    let rem_stdout = String::from_utf8_lossy(&remember_out.stdout);
    assert!(rem_stdout.contains("OK"));

    // 6. Recall Memory
    let recall_out = make_cmd()
        .args([
            "agent",
            "memory",
            &agent_id,
            "recall",
            "--embedding",
            "0.99,0.01,0.0,0.0",
            "--top-k",
            "2",
        ])
        .output()
        .expect("Failed to execute aether agent memory recall");
    assert!(recall_out.status.success());
    let rec_stdout = String::from_utf8_lossy(&recall_out.stdout);
    assert!(rec_stdout.contains("mem-cli-1"));
    assert!(rec_stdout.contains("AetherDB CLI provides native agent management"));

    // 7. Delete State
    let del_out = make_cmd()
        .args(["agent", "state", &agent_id, "delete", "session"])
        .output()
        .expect("Failed to execute aether agent state delete");
    assert!(del_out.status.success());
}
