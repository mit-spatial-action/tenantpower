# Pipelines

## 🐉 Here be dragons! 🐉 

__No attempt has been made to resuscitate this code__. As much as I appreciate your interest and support, no pull requests will be accepted. Channel those energies into [more recent efforts](https://github.com/mit-spatial-action/whoownsmass-frontend).

## Contents

+ [`parcel_scrape.R`](https://github.com/mit-spatial-action/tenantpower.org/blob/main/pipelines/parcel_scrape.R): This was in the bad old days before [MassGIS had released its standardized assessors' parcel database](https://www.mass.gov/info-details/massgis-data-property-tax-parcels). Boston had no way of downloading its parcel database, so I wrote a scraper. The [ArcGIS MapServer is still running](https://gis.boston.gov/arcgis/rest/services/Planning/OpenData/MapServer/9/query?where=1%3D1&outFields=OBJECTID&outSR=102100&f=json), don't know if scraper functions.
+ [`dedupe/`](https://github.com/mit-spatial-action/tenantpower.org/tree/main/pipelines/dedupe): Jupyter notebook and training results ([`dedupe/training`](https://github.com/mit-spatial-action/tenantpower.org/tree/main/pipelines/dedupe)) for an entity resolution workflow that deduplicated landlords based on listed names and addresses using [the dedupe Python library](https://github.com/dedupeio/dedupe). For anyone who's curious this is essentially the method used by [Immergluck et al. 2021](https://doi.org/10.1080/02673037.2019.1639635).

## References

Immergluck, Dan, Jeff Ernsthausen, Stephanie Earl, and Allison Powell. 2020. “Evictions, Large Owners, and Serial Filings: Findings from Atlanta.” _Housing Studies_ 35 (5): 903–24. https://doi.org/10.1080/02673037.2019.1639635.
