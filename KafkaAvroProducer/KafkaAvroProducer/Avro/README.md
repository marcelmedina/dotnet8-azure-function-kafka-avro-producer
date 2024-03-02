#### Avro

You can generate the specific classes in this example using the `avrogen` tool:

```
dotnet tool install --global Apache.Avro.Tools
```

Usage:

Generate the class for the `ExtraOrdinaryPerson` schema:

```
avrogen -s ExtraOrdinaryPerson.avsc ."
```