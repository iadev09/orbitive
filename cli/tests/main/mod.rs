use clap::Parser;

use super::{Cli, Command, human_size};

#[test]
fn fleets_accepts_optional_uid_without_a_fleet_name() {
    let cli = Cli::try_parse_from(["orbit", "fleets"]).unwrap();
    assert!(matches!(cli.command, Command::Fleets(args) if args.uid.is_none()));
    let cli = Cli::try_parse_from(["orbit", "fleets", "--uid", "501"]).unwrap();
    assert!(
        matches!(cli.command, Command::Fleets(args) if args.uid == Some(501))
    );
    assert!(Cli::try_parse_from(["orbit", "fleets", "example"]).is_err());
}

#[test]
fn formats_binary_sizes() {
    assert_eq!(human_size(900), "900 B");
    assert_eq!(human_size(3_200), "3.12 KiB");
    assert_eq!(human_size(3_355_443), "3.20 MiB");
}
