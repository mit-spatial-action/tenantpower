<script>
    import { config } from "$lib/config";
    const { canonical } = config;
</script>

# About

Tenant Power uses machine learning and owner data to match property owners. While powerful, this approach has limitations in identifying exact matches and cannot catch owners using multiple, distinct business names.

The application was built using [Mapbox](https://www.mapbox.com/), [node.js](https://nodejs.org/en/)/[Express](https://expressjs.com/), and [PostgreSQL](https://www.postgresql.org/)/[PostGIS](https://postgis.net/). Source code for the [web app](https://gitlab.com/ericrobskyhuntley/tenant-power) and [data tools](https://gitlab.com/ericrobskyhuntley/landlord-deduplication) (using [dedupe](https://github.com/dedupeio/dedupe)) is available on GitLab.

Data sources include assessor's records from [Boston](https://data.boston.gov/dataset/property-assessment), [Cambridge](https://data.cambridgema.gov/Assessing/Cambridge-Property-Database-FY16-FY20/eey2-rv59), [Somerville](https://data.somervillema.gov/Finance/Assessor-s-Property-Database-FY14-FY19/ubdh-uik5), [MassGIS](https://docs.digital.mass.gov/dataset/massgis-data-standardized-assessors-parcels/), and [Brookline](https://www.mapsonline.net/brooklinema/#x=-7928411.358035,5204910.232188,-7915283.298429,5213910.692267).

This project is a collaboration between [MAMAS](https://mutualaidmamas.com/) and [Graphe](https://geo-graphe.org).
