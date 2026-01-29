# Grundursache: Avro-Konvertierung NullPointerException

## Fehler
```
java.lang.NullPointerException: Cannot invoke "org.apache.avro.Schema$Field.schema()" because "theField" is null
    at io.confluent.connect.avro.AvroData.fromConnectData(AvroData.java:628)
```

## Grundursache

Die Confluent `AvroData`-Bibliothek schlägt beim Konvertieren von Kafka Connect-Schemas nach Avro fehl, wenn **doppelte Feldnamen irgendwo im XSD-Schema existieren**, selbst über verschiedene komplexe Typen hinweg.

### Warum es fehlschlägt

1. **XSD → JAXB → Connect Schema**: Funktioniert korrekt, erstellt gültige Kafka Connect Struct
2. **Connect Schema → Avro Schema**: Funktioniert korrekt, generiert Avro-Schema
3. **Connect Data → Avro Data**: **SCHLÄGT FEHL** - AvroData kann Feldreferenzen nicht auflösen, wenn doppelte Namen vorhanden sind

### Kategorien von Duplikaten

Doppelte Feldnamen können auf verschiedene Arten auftreten:

1. **Untergeordnete Felder in verschiedenen komplexen Typen**:
   ```xml
   <baseRate><amount>10</amount></baseRate>
   <totalAmount><amount>15</amount></totalAmount>
   <!-- Beide haben ein Kindelement namens "amount" -->
   ```

2. **Übergeordnete Elementnamen auf derselben Ebene**:
   ```xml
   <origin><address>...</address></origin>
   <destination><address>...</address></destination>
   <!-- Beide haben ein Kindelement namens "address" -->
   ```

3. **Felder über verschachtelte Strukturen hinweg**:
   ```xml
   <origin><address><city>Portland</city></address></origin>
   <destination><address><city>Seattle</city></address></destination>
   <currentLocation><city>Boise</city></currentLocation>
   <!-- "city" erscheint 3 Mal -->
   ```

## Lösung

**ALLE doppelten Feldnamen müssen umbenannt werden**, um im gesamten Schema eindeutig zu sein:

```xml
<!-- VORHER (schlägt fehl) -->
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

<!-- NACHHER (funktioniert) -->
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

## Wichtige Erkenntnis

**Teilweise Korrekturen funktionieren nicht** - wenn auch nur EIN doppelter Feldname irgendwo im Schema verbleibt, schlägt die Konvertierung mit derselben NullPointerException fehl.

## Verifizierung

Das Problem wurde nachgewiesen durch:
1. ✅ Vereinfachtes Schema OHNE Duplikate → Funktioniert
2. ❌ Komplexes Schema mit Duplikaten → Schlägt fehl
3. ❌ Teilweise korrigiertes Schema (einige Duplikate umbenannt) → Schlägt weiterhin fehl
4. ✅ Vollständig korrigiertes Schema (alle Duplikate umbenannt) → Funktioniert

## Dateien

- `shipments-simple.xsd` - Vereinfachtes Schema, keine Duplikate, **funktioniert**
- `shipments-fixed.xsd` - Vollständiges Schema, alle Duplikate umbenannt, **funktioniert**
- Tests in `Xsd2avroControllerTest.java` verifizieren beide Szenarien
