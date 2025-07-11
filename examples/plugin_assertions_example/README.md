# Plugin Assertions Example

This example demonstrates how to use plugin-based assertion rules using the
`dbt-assertions` framework. Plugins allow you to define reusable rules by writing
`macros` prefixed with `dbt_assertions__*`, such as `dbt_assertions__positive` or
`dbt_assertions__is_in`.

These macros are automatically discovered by the package when their corresponding
helper keys (e.g. `__positive__`) are used in the `assertions:` section of your YAML.

---

### Example usage

In this example we validate an `orders` dataset that includes numeric values and categorical fields.
We want to assert:

- Amount and tax must be positive → `__positive__`
- Payment method must be in a known picklist → `__is_in__`

```yml
version: 2

models:
  - name: d_orders
    tests:
      - dbt_assertions.generic_assertions:
          column: exceptions
          include_list:
            - amount__positive
            - tax__positive
            - payment_method__is_in
          # `re_assert: true` to use only if your assertion's column
          # is not computed and saved in your table.
          re_assert: true

    columns:
      - name: id
      - name: amount
      - name: tax
      - name: payment_method
      - name: exceptions
        assertions:
          __positive__:
            - amount
            - tax

          __is_in__:
            payment_method: ['CREDIT_CARD', 'PAYPAL']
```

This configuration triggers the macros:
- `dbt_assertions__positive`: generates rules `amount__positive`, `tax__positive`
- `dbt_assertions__is_in`: generates rule `payment_method__is_in`

These are automatically included in `assertions()` and validated using `generic_assertions()` with `re_assert: true`.

---

### Files

* `models/d_orders.sql`: synthetic test data
* `models/schema.yml`: plugin helper-based assertions
* `macros/dbt_assertions__positive.sql`: plugin macro to assert `> 0`
* `macros/dbt_assertions__is_in.sql`: plugin macro for allowed values

---

### How to run this example

```bash
dbt build --select examples/plugin_assertions_example
```

This will:

* Compile and run the `d_orders` model with row-level exception logic.
* Recompute assertions using `dbt_assertions.assertions()` inside the test.
* Validate the results using `generic_assertions`.

---

> Happy testing with plugin-based assertions!
