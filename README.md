# Tenant Power

[![Deploy Workflow](https://github.com/mit-spatial-action/tenantpower.org/actions/workflows/deploy.yml/badge.svg)](https://github.com/mit-spatial-action/tenantpower.org/actions/workflows/deploy.yml) [![Netlify Status](https://api.netlify.com/api/v1/badges/b0c5c95a-e249-4527-85e1-02659b27a62a/deploy-status)](https://app.netlify.com/projects/tenantpower/deploys) [![License: GPL v3](https://img.shields.io/badge/License-GPL%20v3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0) [![Project Status: Inactive – The project has reached a stable, usable state but is no longer being actively developed; support/maintenance will be provided as time allows.](https://www.repostatus.org/badges/latest/inactive.svg)](https://www.repostatus.org/#inactive) [![DOI](https://zenodo.org/badge/892278566.svg)](https://doi.org/10.5281/zenodo.18851602)



Monorepo for Tenant Power application, no longer under development but maintained for archival purposes. Superceded by [who-owns-mass-frontend](https://github.com/mit-spatial-action/who-owns-mass-frontend) from [mit-spatial-action](https://github.com/mit-spatial-action). 

The tool operationalized an entity resolution workflow using [the dedupe Python library](https://github.com/dedupeio/dedupe). that deduplicated landlords based on listed names and addresses. For anyone who's curious this is essentially the method used by [Immergluck et al. 2020](https://doi.org/10.1080/02673037.2019.1639635).

## Repo Contents

+ [`app`](https://github.com/mit-spatial-action/tenantpower.org/tree/main/app): Frontend. Svelte/SvelteKit Typescript application. Originally written in vanilla HTML/JS/CSS.
+ [`backend`](https://github.com/mit-spatial-action/tenantpower.org/tree/main/backend): Backend. FastAPI (Python) application built on top of a PostgreSQL/PostGIS database. Originally written in Express.
+ [`pipelines`](https://github.com/mit-spatial-action/tenantpower.org/tree/main/pipelines): Data pipelines including trained [dedupe](https://github.com/dedupeio/dedupe) model.

  > ⚠️ __Dead code alert!__ ⚠️ No attempt has been made to resuscitate `pipelines` for archiving.

## References

Immergluck, Dan, Jeff Ernsthausen, Stephanie Earl, and Allison Powell. 2020. “Evictions, Large Owners, and Serial Filings: Findings from Atlanta.” _Housing Studies_ 35 (5): 903–24. https://doi.org/10.1080/02673037.2019.1639635.