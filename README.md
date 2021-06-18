# ARCHIVED

This repo is deprecated in favour of [tailsdotcom/tap-tableau-server](https://github.com/tailsdotcom/tap-tableau-server), which is refactored to make use of new
features in the Singer SDK, and is more discoverable (as it conforms to the `tap-<tap_name>` naming convention) 👍

## Welcome to the tap-tableau-wrangler Singer Tap!

Tableau Wrangler was created from a need to better manage our Tableau Server
instance, in conjunction with other tools in our data stack including [dbt](https://www.getdbt.com/).

Currently Tableau Wrangler is focused on extracting details embedded in
Workbook files (Datasources, Connections and Relations) as well as retrieving
table references from embedded Custom SQL text fields inside Relation entities.

This may not mean much to you (unless you are a seasoned Tableau-er) but the
above details can help answer questions like:

- Which Workbooks depend on which tables in which databases?
- How many Workbooks depend on Excel, CSV or Google Sheets?
- Who's credentials are used for embedded connections, in which Workbooks?

Having answers to all of these questions has helped us with wrangling our
Tableau Server instance, which has several 100's of Workbooks.

In future, we hope to extend this tap to cover other metadata that is exposed
by the Tableau Server API's directly (esp. Published Connections and child objects).
PR submission very welcome. Watch this space!

This Singer-compliant tap was created using the [Singer SDK](https://gitlab.com/meltano/singer-sdk).

## Singer SDK Dev Guide

See the [dev guide](../../docs/dev_guide.md) for more instructions on how to use the Singer SDK to
develop your own taps and targets.

## Config Guide

This tap requires the following config:

```json
{
    "username": "example.username",
    "password": "mysupersecretpassword",
    "host": "https://tableau.com",
    "batch_size": 50
}
```

`batch_size` is optional and represents the number of Workbooks to fetch in a single
run. We found it necessary to add this to control the load on our Tableau Server;
the tap will only download `batch_size` records at a time, in order of oldest
modified (since the last bookmark, if a bookmark is available from a previous run).
This ensures tap runs do not adversely affect regular users of Tableau Server/Web.
