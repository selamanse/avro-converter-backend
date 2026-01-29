# Root Cause: Avro Conversion NullPointerException

## Error
```
java.lang.NullPointerException: Cannot invoke "org.apache.avro.Schema$Field.schema()" because "theField" is null
    at io.confluent.connect.avro.AvroData.fromConnectData(AvroData.java:628)
```

## Root Cause

The Confluent `AvroData` library fails when converting Kafka Connect schemas to Avro if **duplicate field names exist anywhere in the XSD schema**, even across different complex types.

### Why It Fails

1. **XSD → JAXB → Connect Schema**: Works correctly, creates valid Kafka Connect Struct
2. **Connect Schema → Avro Schema**: Works correctly, generates Avro schema
3. **Connect Data → Avro Data**: **FAILS** - AvroData cannot resolve field references when duplicate names exist

### Duplicate Categories

Duplicate field names can appear in multiple ways:

1. **Child fields in different complex types**:
   ```xml
   <baseRate><amount>10</amount></baseRate>
   <totalAmount><amount>15</amount></totalAmount>
   <!-- Both have child named "amount" -->
   ```

2. **Parent element names at same level**:
   ```xml
   <origin><address>...</address></origin>
   <destination><address>...</address></destination>
   <!-- Both have child element named "address" -->
   ```

3. **Fields across nested structures**:
   ```xml
   <origin><address><city>Portland</city></address></origin>
   <destination><address><city>Seattle</city></address></destination>
   <currentLocation><city>Boise</city></currentLocation>
   <!-- "city" appears 3 times -->
   ```

## Solution

**ALL duplicate field names must be renamed** to be unique across the entire schema:

```xml
<!-- BEFORE (fails) -->
<origin>
    <address>
        <city>Portland</city>
    </address>
</origin>
<destination>
    <address>
        <city>Seattle</city>
    </address>
</destination>

<!-- AFTER (works) -->
<origin>
    <originAddress>
        <originCity>Portland</originCity>
    </originAddress>
</origin>
<destination>
    <destinationAddress>
        <destinationCity>Seattle</destinationCity>
    </destinationAddress>
</destination>
```

## Key Insight

**Partial fixes don't work** - if even ONE duplicate field name remains anywhere in the schema, the conversion fails with the same NullPointerException.

## Verification

The issue was proven by:
1. ✅ Simplified schema with NO duplicates → Works
2. ❌ Complex schema with duplicates → Fails
3. ❌ Partially fixed schema (some duplicates renamed) → Still fails
4. ✅ Fully fixed schema (all duplicates renamed) → Works

## Files

- `shipments-simple.xsd` - Simplified schema, no duplicates, **works**
- `shipments-fixed.xsd` - Full schema, all duplicates renamed, **works**
- Tests in `Xsd2avroControllerTest.java` verify both scenarios
