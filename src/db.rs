// Copyright (C) 2022  Aravinth Manivannan <realaravinth@batsense.net>
// SPDX-FileCopyrightText: 2023 Aravinth Manivannan <realaravinth@batsense.net>
//
// SPDX-License-Identifier: AGPL-3.0-or-later

use crate::settings::Settings;
use db_core::prelude::*;
use std::time::Duration;

pub type BoxDB = Box<dyn MCDatabase>;

const MIGRATION_RETRY_INTERVAL: Duration = Duration::from_secs(5);

/// Run migrations now if the database is reachable. Otherwise keep retrying
/// in the background so the server can start (and serve PoW from Redis)
/// while the database is down.
async fn migrate_or_retry<D>(db: D)
where
    D: Migrate + Send + Sync + 'static,
{
    if let Err(e) = db.migrate().await {
        log::error!("database migration failed, retrying in background: {e}");
        actix_web::rt::spawn(async move {
            loop {
                tokio::time::sleep(MIGRATION_RETRY_INTERVAL).await;
                match db.migrate().await {
                    Ok(()) => {
                        log::info!("database migration succeeded");
                        break;
                    }
                    Err(e) => log::warn!("database migration retry failed: {e}"),
                }
            }
        });
    }
}

pub mod pg {
    use super::*;
    use db_sqlx_postgres::{Conn, ConnectionOptions};
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
    use sqlx::ConnectOptions;
    use std::str::FromStr;

    pub async fn get_data(settings: Option<Settings>) -> BoxDB {
        let settings = settings.unwrap_or_else(|| Settings::new().unwrap());
        let pool = settings.database.pool;
        let mut connect_options =
            PgConnectOptions::from_str(&settings.database.url).unwrap();
        if !settings.debug {
            connect_options = connect_options.disable_statement_logging();
        }
        // Lazy pool: connections are opened on first use, so startup does not
        // depend on the database being up.
        let pool = PgPoolOptions::new()
            .min_connections(0)
            .max_connections(pool)
            .acquire_timeout(Duration::from_secs(2))
            .idle_timeout(Some(Duration::from_secs(60)))
            .max_lifetime(Some(Duration::from_secs(30 * 60)))
            .connect_lazy_with(connect_options);
        let db = ConnectionOptions::Existing(Conn(pool))
            .connect()
            .await
            .unwrap();
        migrate_or_retry(db.clone()).await;
        Box::new(db)
    }
}

pub mod maria {
    use super::*;
    use db_sqlx_maria::{Conn, ConnectionOptions};
    use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions};
    use sqlx::ConnectOptions;
    use std::str::FromStr;

    pub async fn get_data(settings: Option<Settings>) -> BoxDB {
        let settings = settings.unwrap_or_else(|| Settings::new().unwrap());
        let pool = settings.database.pool;
        let mut connect_options =
            MySqlConnectOptions::from_str(&settings.database.url).unwrap();
        if !settings.debug {
            connect_options = connect_options.disable_statement_logging();
        }
        // Lazy pool: connections are opened on first use, so startup does not
        // depend on the database being up.
        let pool = MySqlPoolOptions::new()
            .min_connections(0)
            .max_connections(pool)
            .acquire_timeout(Duration::from_secs(2))
            .idle_timeout(Some(Duration::from_secs(60)))
            .max_lifetime(Some(Duration::from_secs(30 * 60)))
            .connect_lazy_with(connect_options);
        let db = ConnectionOptions::Existing(Conn(pool))
            .connect()
            .await
            .unwrap();
        migrate_or_retry(db.clone()).await;
        Box::new(db)
    }
}
