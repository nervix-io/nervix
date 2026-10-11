#![cfg_attr(nervix_lint, nervix::layer(data_plane, reason="typed plan execution"))]
pub fn execute() {
    let value = callee::Renamed::Ready;
    assert!(matches!(value, callee::Renamed::Ready));
    assert_eq!(callee::parse(), 0);
}
