use std::sync::Arc;

use anyhow::{Result, ensure};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use chrono::{DateTime, Timelike, Utc};

use crate::config::validate_identifier;

const ASSET_ID_ALIAS: &str = "JSONExtractString(data, 'asset_id') AS asset_id_text";
const ASSET_ID_COLUMN: &str = "toFixedString(\n    unhex(leftPad(hex(toUInt256OrZero(asset_id_text)), 64, '0')),\n    32\n) AS asset_id";
const ASSET_ID_VALIDATION: &str = "throwIf(\n    NOT match(asset_id_text, '^(0|[1-9][0-9]{0,77})$')\n        OR toString(toUInt256OrZero(asset_id_text)) != asset_id_text,\n    'invalid Polymarket asset ID'\n) = 0";
const ASSETS_IDS_ALIAS: &str =
    "JSONExtract(data, 'assets_ids', 'Array(String)') AS assets_ids_text";
const ASSETS_IDS_COLUMN: &str = "arrayMap(\n    value -> toFixedString(\n        unhex(leftPad(hex(toUInt256OrZero(value)), 64, '0')),\n        32\n    ),\n    assets_ids_text\n) AS assets_ids";
const ASSETS_IDS_VALIDATION: &str = "throwIf(\n    arrayExists(\n        value -> NOT match(value, '^(0|[1-9][0-9]{0,77})$')\n            OR toString(toUInt256OrZero(value)) != value,\n        assets_ids_text\n    ),\n    'invalid Polymarket lifecycle asset ID'\n) = 0";

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum EventType {
    Book,
    PriceChange,
    LastTradePrice,
    TickSizeChange,
    BestBidAsk,
    NewMarket,
    MarketResolved,
}

impl EventType {
    pub const ALL: [Self; 7] = [
        Self::Book,
        Self::PriceChange,
        Self::LastTradePrice,
        Self::TickSizeChange,
        Self::BestBidAsk,
        Self::NewMarket,
        Self::MarketResolved,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Book => "book",
            Self::PriceChange => "price_change",
            Self::LastTradePrice => "last_trade_price",
            Self::TickSizeChange => "tick_size_change",
            Self::BestBidAsk => "best_bid_ask",
            Self::NewMarket => "new_market",
            Self::MarketResolved => "market_resolved",
        }
    }

    pub const fn sort_columns(self) -> &'static [&'static str] {
        match self {
            Self::NewMarket | Self::MarketResolved => &["market", "sequence"],
            _ => &["market", "asset_id", "sequence"],
        }
    }

    pub const fn dictionary_columns(self) -> &'static [&'static str] {
        match self {
            Self::PriceChange => &["side"],
            Self::LastTradePrice => &["price", "side", "fee_rate_bps"],
            _ => &[],
        }
    }

    pub fn schema(self) -> SchemaRef {
        let mut fields = common_fields();
        fields.extend(self.event_fields());
        Arc::new(Schema::new(fields))
    }

    fn event_fields(self) -> Vec<Field> {
        let required = |name, data_type| Field::new(name, data_type, false);
        let optional = |name, data_type| Field::new(name, data_type, true);
        match self {
            Self::Book => vec![
                required("asset_id", DataType::FixedSizeBinary(32)),
                required("bids", order_levels_type()),
                required("asks", order_levels_type()),
            ],
            Self::PriceChange => vec![
                required("asset_id", DataType::FixedSizeBinary(32)),
                required("price", DataType::Decimal32(9, 4)),
                required("size", DataType::Decimal64(18, 6)),
                required("side", DataType::Utf8),
                optional("best_bid", DataType::Decimal32(9, 4)),
                optional("best_ask", DataType::Decimal32(9, 4)),
            ],
            Self::LastTradePrice => vec![
                required("asset_id", DataType::FixedSizeBinary(32)),
                required("price", DataType::Decimal32(9, 4)),
                required("size", DataType::Decimal64(18, 6)),
                required("side", DataType::Utf8),
                required("fee_rate_bps", DataType::UInt16),
                required("transaction_hash", DataType::FixedSizeBinary(32)),
            ],
            Self::TickSizeChange => vec![
                required("asset_id", DataType::FixedSizeBinary(32)),
                required("old_tick_size", DataType::Decimal32(9, 4)),
                required("new_tick_size", DataType::Decimal32(9, 4)),
            ],
            Self::BestBidAsk => vec![
                required("asset_id", DataType::FixedSizeBinary(32)),
                optional("best_bid", DataType::Decimal32(9, 4)),
                optional("best_ask", DataType::Decimal32(9, 4)),
                optional("spread", DataType::Decimal32(9, 4)),
            ],
            Self::NewMarket => vec![
                required("id", DataType::Utf8),
                required("assets_ids", fixed_binary_list_type()),
                required("outcomes", string_list_type()),
                optional("question", DataType::Utf8),
                optional("slug", DataType::Utf8),
            ],
            Self::MarketResolved => vec![
                required("id", DataType::Utf8),
                required("assets_ids", fixed_binary_list_type()),
                optional("winning_asset_id", DataType::FixedSizeBinary(32)),
                optional("winning_outcome", DataType::Utf8),
            ],
        }
    }

    fn aliases(self) -> &'static [&'static str] {
        match self {
            Self::NewMarket => &[ASSETS_IDS_ALIAS],
            Self::MarketResolved => &[
                ASSETS_IDS_ALIAS,
                "JSONExtractString(data, 'winning_asset_id') AS winning_asset_id_text",
            ],
            Self::LastTradePrice => &[
                ASSET_ID_ALIAS,
                "JSONExtractString(data, 'transaction_hash') AS transaction_hash_text",
            ],
            _ => &[ASSET_ID_ALIAS],
        }
    }

    fn columns(self) -> &'static [&'static str] {
        match self {
            Self::Book => &[
                ASSET_ID_COLUMN,
                "JSONExtract(data, 'bids', 'Array(Tuple(price Decimal32(4), size Decimal64(6)))') AS bids",
                "JSONExtract(data, 'asks', 'Array(Tuple(price Decimal32(4), size Decimal64(6)))') AS asks",
            ],
            Self::PriceChange => &[
                ASSET_ID_COLUMN,
                "toDecimal32OrZero(JSONExtractString(data, 'price'), 4) AS price",
                "toDecimal64OrZero(JSONExtractString(data, 'size'), 6) AS size",
                "JSONExtractString(data, 'side') AS side",
                "toDecimal32OrNull(JSONExtractString(data, 'best_bid'), 4) AS best_bid",
                "toDecimal32OrNull(JSONExtractString(data, 'best_ask'), 4) AS best_ask",
            ],
            Self::LastTradePrice => &[
                ASSET_ID_COLUMN,
                "toDecimal32OrZero(JSONExtractString(data, 'price'), 4) AS price",
                "toDecimal64OrZero(JSONExtractString(data, 'size'), 6) AS size",
                "JSONExtractString(data, 'side') AS side",
                "toUInt16OrZero(JSONExtractString(data, 'fee_rate_bps')) AS fee_rate_bps",
                "toFixedString(unhex(substring(transaction_hash_text, 3)), 32) AS transaction_hash",
            ],
            Self::TickSizeChange => &[
                ASSET_ID_COLUMN,
                "toDecimal32OrZero(JSONExtractString(data, 'old_tick_size'), 4) AS old_tick_size",
                "toDecimal32OrZero(JSONExtractString(data, 'new_tick_size'), 4) AS new_tick_size",
            ],
            Self::BestBidAsk => &[
                ASSET_ID_COLUMN,
                "toDecimal32OrNull(JSONExtractString(data, 'best_bid'), 4) AS best_bid",
                "toDecimal32OrNull(JSONExtractString(data, 'best_ask'), 4) AS best_ask",
                "toDecimal32OrNull(JSONExtractString(data, 'spread'), 4) AS spread",
            ],
            Self::NewMarket => &[
                "JSONExtractString(data, 'id') AS id",
                ASSETS_IDS_COLUMN,
                "JSONExtract(data, 'outcomes', 'Array(String)') AS outcomes",
                "if(JSONHas(data, 'question'), JSONExtractString(data, 'question'), NULL) AS question",
                "if(JSONHas(data, 'slug'), JSONExtractString(data, 'slug'), NULL) AS slug",
            ],
            Self::MarketResolved => &[
                "JSONExtractString(data, 'id') AS id",
                ASSETS_IDS_COLUMN,
                "if(\n    winning_asset_id_text != '',\n    toFixedString(\n        unhex(leftPad(hex(toUInt256OrZero(winning_asset_id_text)), 64, '0')),\n        32\n    ),\n    NULL\n) AS winning_asset_id",
                "nullIf(JSONExtractString(data, 'winning_outcome'), '') AS winning_outcome",
            ],
        }
    }

    fn validations(self) -> &'static [&'static str] {
        match self {
            Self::NewMarket => &[ASSETS_IDS_VALIDATION],
            Self::MarketResolved => &[
                ASSETS_IDS_VALIDATION,
                "throwIf(\n    winning_asset_id_text != ''\n        AND (\n            NOT match(winning_asset_id_text, '^(0|[1-9][0-9]{0,77})$')\n            OR toString(toUInt256OrZero(winning_asset_id_text))\n                != winning_asset_id_text\n        ),\n    'invalid Polymarket winning asset ID'\n) = 0",
            ],
            Self::LastTradePrice => &[
                ASSET_ID_VALIDATION,
                "throwIf(\n    NOT match(transaction_hash_text, '^0[xX][0-9a-fA-F]{64}$'),\n    'invalid Polymarket transaction hash'\n) = 0",
            ],
            _ => &[ASSET_ID_VALIDATION],
        }
    }
}

impl std::fmt::Display for EventType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

fn common_fields() -> Vec<Field> {
    vec![
        Field::new(
            "timestamp_received",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("sequence", DataType::UInt64, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        Field::new("market", DataType::FixedSizeBinary(32), false),
    ]
}

fn order_levels_type() -> DataType {
    let fields: Fields = vec![
        Field::new("price", DataType::Decimal32(9, 4), false),
        Field::new("size", DataType::Decimal64(18, 6), false),
    ]
    .into();
    DataType::List(Arc::new(Field::new(
        "element",
        DataType::Struct(fields),
        false,
    )))
}

fn fixed_binary_list_type() -> DataType {
    DataType::List(Arc::new(Field::new(
        "element",
        DataType::FixedSizeBinary(32),
        false,
    )))
}

fn string_list_type() -> DataType {
    DataType::List(Arc::new(Field::new("element", DataType::Utf8, false)))
}

pub fn ensure_utc_hour(hour: DateTime<Utc>) -> Result<()> {
    ensure!(
        hour.minute() == 0 && hour.second() == 0 && hour.nanosecond() == 0,
        "timestamp is not an exact UTC hour: {hour}"
    );
    Ok(())
}

pub fn build_event_query(hour: DateTime<Utc>, event: EventType, table: &str) -> Result<String> {
    ensure_utc_hour(hour)?;
    validate_identifier("CLICKHOUSE_TABLE", table)?;
    let target = hour.format("%Y-%m-%d %H:00:00");
    let aliases = event
        .aliases()
        .iter()
        .map(|alias| format!(",\n    {alias}"))
        .collect::<String>();
    let columns = event.columns().join(",\n    ");
    let validations = event
        .validations()
        .iter()
        .map(|validation| format!("\n  AND {validation}"))
        .collect::<String>();
    let order_by = event.sort_columns().join(", ");
    Ok(format!(
        "\nWITH\n    JSONExtractString(data, 'market') AS market_text{aliases}\nSELECT\n    timestamp_received,\n    sequence,\n    fromUnixTimestamp64Milli(\n        toInt64OrZero(JSONExtractString(data, 'timestamp')),\n        'UTC'\n    ) AS timestamp,\n    toFixedString(unhex(substring(market_text, 3)), 32) AS market,\n    {columns}\nFROM {table} FINAL\nWHERE timestamp_received >= toDateTime64('{target}', 9, 'UTC')\n  AND timestamp_received <  toDateTime64('{target}', 9, 'UTC') + INTERVAL 1 HOUR\n  AND JSONExtractString(data, 'event_type') = '{event}'\n  AND throwIf(\n      NOT match(market_text, '^0[xX][0-9a-fA-F]{{64}}$'),\n      'invalid Polymarket condition ID'\n  ) = 0{validations}\nORDER BY {order_by}\nSETTINGS do_not_merge_across_partitions_select_final = 1\nFORMAT ArrowStream\n"
    ))
}

#[cfg(test)]
mod tests {
    use arrow_schema::TimeUnit;
    use chrono::TimeZone;

    use super::*;

    fn hour() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 13, 4, 0, 0).unwrap()
    }

    #[test]
    fn all_event_schemas_are_explicit_and_narrow() {
        for event in EventType::ALL {
            let schema = event.schema();
            assert_eq!(
                schema
                    .field_with_name("timestamp_received")
                    .unwrap()
                    .data_type(),
                &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
            );
            assert_eq!(
                schema.field_with_name("sequence").unwrap().data_type(),
                &DataType::UInt64
            );
            assert_eq!(
                schema.field_with_name("market").unwrap().data_type(),
                &DataType::FixedSizeBinary(32)
            );
            assert!(
                schema
                    .fields()
                    .iter()
                    .take(4)
                    .all(|field| !field.is_nullable())
            );
        }

        let book = EventType::Book.schema();
        assert_eq!(
            book.field_with_name("bids").unwrap().data_type(),
            &order_levels_type()
        );
        assert_eq!(
            book.field_with_name("asks").unwrap().data_type(),
            &order_levels_type()
        );
        let trade = EventType::LastTradePrice.schema();
        assert_eq!(
            trade.field_with_name("price").unwrap().data_type(),
            &DataType::Decimal32(9, 4)
        );
        assert_eq!(
            trade.field_with_name("size").unwrap().data_type(),
            &DataType::Decimal64(18, 6)
        );
        assert_eq!(
            trade.field_with_name("fee_rate_bps").unwrap().data_type(),
            &DataType::UInt16
        );
        assert_eq!(
            trade
                .field_with_name("transaction_hash")
                .unwrap()
                .data_type(),
            &DataType::FixedSizeBinary(32)
        );

        let price_change = EventType::PriceChange.schema();
        assert!(
            price_change
                .field_with_name("best_bid")
                .unwrap()
                .is_nullable()
        );
        assert!(
            price_change
                .field_with_name("best_ask")
                .unwrap()
                .is_nullable()
        );
        let resolution = EventType::MarketResolved.schema();
        assert!(
            resolution
                .field_with_name("winning_asset_id")
                .unwrap()
                .is_nullable()
        );
        assert!(
            resolution
                .field_with_name("winning_outcome")
                .unwrap()
                .is_nullable()
        );
        assert!(resolution.field_with_name("asset_id").is_err());
    }

    #[test]
    fn queries_preserve_typed_projections_validation_and_sorting() {
        let book = build_event_query(hour(), EventType::Book, "events").unwrap();
        let level_type = "Array(Tuple(price Decimal32(4), size Decimal64(6)))";
        assert!(book.contains(&format!(
            "JSONExtract(data, 'bids', '{level_type}') AS bids"
        )));
        assert!(book.contains("FROM events FINAL"));
        assert!(book.contains("ORDER BY market, asset_id, sequence"));
        assert!(book.contains("do_not_merge_across_partitions_select_final = 1"));
        assert!(book.contains("FORMAT ArrowStream"));
        assert!(book.contains("invalid Polymarket condition ID"));
        assert!(book.contains("invalid Polymarket asset ID"));

        let trade = build_event_query(hour(), EventType::LastTradePrice, "events").unwrap();
        assert!(trade.contains("invalid Polymarket transaction hash"));
        assert!(trade.contains("toUInt16OrZero"));

        let lifecycle = build_event_query(hour(), EventType::MarketResolved, "events").unwrap();
        assert!(lifecycle.contains("ORDER BY market, sequence"));
        assert!(lifecycle.contains("invalid Polymarket lifecycle asset ID"));
        assert!(lifecycle.contains("winning_asset_id_text != ''"));
        assert!(lifecycle.contains("nullIf(JSONExtractString(data, 'winning_outcome'), '')"));
    }

    #[test]
    fn non_hour_timestamp_is_rejected() {
        let minute = Utc.with_ymd_and_hms(2026, 8, 13, 4, 1, 0).unwrap();
        assert!(build_event_query(minute, EventType::Book, "events").is_err());
        assert!(build_event_query(hour(), EventType::Book, "events; DROP TABLE events").is_err());
    }

    #[test]
    fn encoding_and_sort_policies_cover_every_event() {
        for event in EventType::ALL {
            assert!(!event.sort_columns().is_empty());
        }
        assert_eq!(EventType::PriceChange.dictionary_columns(), &["side"]);
        assert_eq!(
            EventType::LastTradePrice.dictionary_columns(),
            &["price", "side", "fee_rate_bps"]
        );
        assert!(EventType::Book.dictionary_columns().is_empty());
    }
}
