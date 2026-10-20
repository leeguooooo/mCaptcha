// Copyright (C) 2021  Aravinth Manivannan <realaravinth@batsense.net>
// SPDX-FileCopyrightText: 2023 Aravinth Manivannan <realaravinth@batsense.net>
//
// SPDX-License-Identifier: AGPL-3.0-or-later

use std::env;
use std::process::Command;

use sqlx::types::time::OffsetDateTime;

fn main() {
    println!("cargo:rerun-if-env-changed=MCAPTCHA_BUILD_GIT_HASH");
    let git_hash = env::var("MCAPTCHA_BUILD_GIT_HASH")
        .ok()
        .filter(|hash| !hash.trim().is_empty())
        .or_else(|| {
            Command::new("git")
                .args(["rev-parse", "HEAD"])
                .output()
                .ok()
                .filter(|output| output.status.success())
                .and_then(|output| String::from_utf8(output.stdout).ok())
        })
        .map(|hash| hash.trim().to_owned())
        .unwrap_or_else(|| "unknown".to_owned());
    println!("cargo:rustc-env=GIT_HASH={}", git_hash);

    let now = OffsetDateTime::now_utc();
    let now = format!("{}{}{}", now.year(), now.month(), now.date());
    println!("cargo:rustc-env=COMPILED_DATE={}", &now);
}
