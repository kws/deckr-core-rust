//! Standard hardware capability schemas implemented from the Deckr reference.

use crate::lanes::CapabilitySchema;
use serde_json::json;

fn capability_schema(schema_id: &str, schema: serde_json::Value) -> CapabilitySchema {
    CapabilitySchema {
        schema_id: Some(schema_id.to_string()),
        schema,
    }
}

pub fn button_activation_value_schema() -> CapabilitySchema {
    capability_schema(
        "dev.deckr.value.input.button.activation.v1",
        json!({
            "type": "object",
            "required": ["eventType"],
            "properties": {"eventType": {"const": "press"}},
            "additionalProperties": false
        }),
    )
}

pub fn button_momentary_value_schema() -> CapabilitySchema {
    capability_schema(
        "dev.deckr.value.input.button.momentary.v1",
        json!({
            "type": "object",
            "required": ["eventType"],
            "properties": {"eventType": {"enum": ["down", "up"]}},
            "additionalProperties": false
        }),
    )
}

pub fn encoder_relative_value_schema() -> CapabilitySchema {
    capability_schema(
        "dev.deckr.value.input.encoder.relative.v1",
        json!({
            "type": "object",
            "required": ["delta"],
            "properties": {
                "delta": {"type": "integer", "not": {"const": 0}},
                "direction": {"enum": ["clockwise", "counterclockwise"]}
            },
            "additionalProperties": false
        }),
    )
}

pub fn touch_gesture_value_schema() -> CapabilitySchema {
    capability_schema(
        "dev.deckr.value.input.touch.gesture.v1",
        json!({
            "oneOf": [
                {
                    "type": "object",
                    "required": ["eventType"],
                    "properties": {"eventType": {"const": "tap"}},
                    "additionalProperties": false
                },
                {
                    "type": "object",
                    "required": ["eventType", "direction"],
                    "properties": {
                        "eventType": {"const": "swipe"},
                        "direction": {"enum": ["left", "right"]}
                    },
                    "additionalProperties": false
                }
            ]
        }),
    )
}

pub fn raster_bitmap_command_schema(width: u32, height: u32) -> CapabilitySchema {
    capability_schema(
        "dev.deckr.command.output.raster.bitmap.v1",
        json!({
            "oneOf": [
                {
                    "type": "object",
                    "required": ["image", "encoding"],
                    "properties": {
                        "image": {"type": "string", "contentEncoding": "base64"},
                        "encoding": {"enum": ["jpeg", "png"]},
                        "width": {"const": width},
                        "height": {"const": height}
                    },
                    "additionalProperties": false
                },
                {"type": "object", "maxProperties": 0}
            ]
        }),
    )
}

pub fn device_power_command_schema() -> CapabilitySchema {
    capability_schema(
        "dev.deckr.command.device.power.screen.v1",
        json!({"type": "object", "maxProperties": 0}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::SchemaRegistry;

    #[test]
    fn standard_schemas_reject_invalid_payloads() {
        for (schema, valid, invalid) in [
            (
                button_activation_value_schema(),
                json!({"eventType":"press"}),
                json!({"eventType":"up"}),
            ),
            (
                button_momentary_value_schema(),
                json!({"eventType":"down"}),
                json!({"eventType":"press"}),
            ),
            (
                encoder_relative_value_schema(),
                json!({"delta":-1}),
                json!({"delta":0}),
            ),
            (
                touch_gesture_value_schema(),
                json!({"eventType":"swipe","direction":"left"}),
                json!({"eventType":"swipe"}),
            ),
            (
                device_power_command_schema(),
                json!({}),
                json!({"power":true}),
            ),
            (
                raster_bitmap_command_schema(64, 64),
                json!({"image":"YWJj","encoding":"png","width":64}),
                json!({"image":"YWJj","encoding":"png","width":32}),
            ),
        ] {
            let compiled = SchemaRegistry::default().compile(&schema.schema).unwrap();
            compiled.validate(&valid).unwrap();
            assert!(compiled.validate(&invalid).is_err());
        }
    }
}
