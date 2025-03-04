# Databricks Markdown Notebook: Python Basics for Data Engineers at Menzis

## 1. Basis Syntaxis

```markdown
```python
# Variabelen en datatypes
aantal_patienten = 1500  # Integer
zorgkosten = 12345.67  # Float
zorginstelling = "Menzis"  # String
print(f"{zorginstelling} heeft {aantal_patienten} patienten met een totale zorgkosten van €{zorgkosten}.")
```
```

## 2. String Expressies

```markdown
```python
# String manipulatie
patient_naam = "Jan Jansen"
print(patient_naam.upper())  # Alles in hoofdletters
print(patient_naam.lower())  # Alles in kleine letters
print(patient_naam.replace("Jan", "Piet"))  # Naam vervangen
```
```

## 3. Number Expressies

```markdown
```python
# Basis wiskunde
medicatie_kosten = 49.99
verzekering_korting = 10  # In procenten
totale_kosten = medicatie_kosten * (1 - verzekering_korting / 100)
print(round(totale_kosten, 2))  # Afronden op 2 decimalen
```
```

## 4. DateTime Expressies

```markdown
```python
from datetime import datetime, timedelta

# Huidige datum en tijd
nu = datetime.now()
print("Huidige datum en tijd:", nu)

# Datum manipuleren (bijv. 30 dagen vooruit)
toekomstige_datum = nu + timedelta(days=30)
print("Toekomstige datum:", toekomstige_datum.strftime('%Y-%m-%d'))
```
```

## 5. Logische Expressies

```markdown
```python
# If-statements
leeftijd = 70
if leeftijd >= 65:
    print("Deze patiënt komt in aanmerking voor extra zorgondersteuning.")
else:
    print("Geen extra ondersteuning vereist.")
```
```

## 6. List Expressies

```markdown
```python
# Werken met lijsten
patiënten = ["Jan", "Piet", "Marie"]
patiënten.append("Karin")  # Toevoegen
print(patiënten)
print(patiënten[1])  # Tweede patiënt
```
```

## 7. Dictionary Expressies

```markdown
```python
# Key-Value Data
patiënt_data = {"naam": "Jan Jansen", "leeftijd": 45, "verzekering": "Basis"}
print(patiënt_data["naam"])
```
```

## 8. Loops

```markdown
```python
# For-loop
for patiënt in patiënten:
    print(f"Patiënt: {patiënt}")

# While-loop
aantal = 0
while aantal < 3:
    print(f"Aantal geregistreerde patiënten: {aantal}")
    aantal += 1
```
```

## 9. PySpark DataFrame Basis

```markdown
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# SparkSession aanmaken
spark = SparkSession.builder.appName("MenzisData").getOrCreate()

# Sample DataFrame
data = [("Jan", 45, "Basis"), ("Marie", 55, "Uitgebreid"), ("Karin", 60, "Basis")]
columns = ["Naam", "Leeftijd", "Verzekering"]
df = spark.createDataFrame(data, columns)

df.show()
```
```

## 10. PySpark Filteren en Selecteren

```markdown
```python
# Patiënten ouder dan 50
ouderen = df.filter(col("Leeftijd") > 50)
ouderen.show()
```
```

## 11. PySpark Groeperen en Aggregaties

```markdown
```python
# Aantal patiënten per verzekeringstype
df.groupBy("Verzekering").count().show()
```
```

## 12. PySpark Joins

```markdown
```python
# Extra dataset maken met medische kosten
kosten_data = [("Jan", 200), ("Marie", 350), ("Karin", 150)]
kosten_columns = ["Naam", "Kosten"]
kosten_df = spark.createDataFrame(kosten_data, kosten_columns)

# Join uitvoeren op Naam
df_kosten = df.join(kosten_df, "Naam", "inner")
df_kosten.show()
```
```
