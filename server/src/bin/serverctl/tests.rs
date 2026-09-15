use super::*;

#[test]
fn parses_repeated_routes_and_normalizes_static_root() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("web");
    let mut arguments = Arguments::new(vec![
        "--route".into(),
        "/api".into(),
        "--host-route".into(),
        "example.test".into(),
        "/live".into(),
        "--root".into(),
        root.display().to_string(),
    ]);
    let options = parse_service_options(&mut arguments, true).unwrap();
    assert_eq!(options.routes.len(), 2);
    assert!(options.root.unwrap().is_absolute());
}

#[test]
fn conflicting_fail_modes_are_rejected() {
    let mut arguments = Arguments::new(vec!["--fail-open".into(), "--fail-closed".into()]);
    assert!(parse_service_options(&mut arguments, false).is_err());
}
