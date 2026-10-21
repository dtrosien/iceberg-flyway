---
name: scalatest-tests
description: How to write and run tests in this project with ScalaTest (UnitSpec = AnyFlatSpec with should.Matchers, OptionValues, Inside; BeforeAndAfterAll for containers). Use when adding, changing or debugging tests.
---

# Writing ScalaTest tests

Tests live in `src/test/scala/...` as classes named `*Test` that extend
`com.example.icebergflyway.UnitSpec`, the same base as in flink-rowdata4s:

```scala
abstract class UnitSpec extends AnyFlatSpec with should.Matchers with OptionValues with Inside
```

## Style

```scala
class IcebergSchemasTest extends UnitSpec:

  "IcebergSchemas" should "derive the expected columns" in {
    IcebergSchemas.columns(IcebergSchemas.schemaFor[User]) shouldBe List(...)
  }

  it should "turn @TableComment into the column doc" in {
    IcebergSchemas.schemaFor[User].findField("id").doc shouldBe "primary key"
  }
```

- `"Subject" should "behaviour" in { ... }` for the first test of a subject, `it should` for
  the following ones.
- Assertions are matchers: `x shouldBe y`, `s should include("...")`, `opt.value shouldBe y`
  (`OptionValues`), `a[TableDriftException] should be thrownBy f()`,
  `noException should be thrownBy f()`.
- There is no effect system: tests call the synchronous code directly.
- Per-test resources go through a helper that cleans up in `finally`, e.g.
  `withCatalog { cat => ... }` in `IcebergMigrationTest`. Suite-wide resources (the Postgres
  container in `MigrationTest`) use `BeforeAndAfterAll`; tests in one suite run sequentially.

## Running

```
sbt testFull                                     # every test (sbt 2's `test` is incremental)
sbt "testOnly *MigrationTest"                    # one suite
sbt "testOnly *MigrationTest -- -z seed"         # only tests whose name contains the text
```
