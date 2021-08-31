# athena-adwords
This [dbt](https://github.com/fishtown-analytics/dbt) package contains macros
that:

- define Athena-specific implementations of [dispatched macros](https://docs.getdbt.com/reference/dbt-jinja-functions/adapter/#dispatch) from the [`adwords`](https://github.com/dbt-labs/adwords) package.

## Installation Instructions

Add to your packages.yml

```yaml
packages:
  - git: https://github.com/Project-J/athena-adwords
    revision: 0.1.0
```

For dbt < v0.19.2, add the following lines to your `dbt_project.yml`:

```yaml
vars:
  dbt_utils_dispatch_list: ["athena_adwords"]
```

For dbt >= v0.19.2, , add the following lines to your `dbt_project.yml`:

```yaml
dispatch:
  - macro_namespace: adwords
    search_order: [athena_adwords, adwords]
```

## Compatibility

This package provides "shims" for [`adwords`](https://github.com/dbt-labs/adwords).
In the future more shims could be added to this repository.

### Contributing

We welcome contributions to this repo! To contribute a new feature or a fix,
please open a Pull Request with 1) your changes and 2) updated documentation for
the `README.md` file.
