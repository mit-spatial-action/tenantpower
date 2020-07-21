require('sf')
require('tidyverse') 
require('urltools')
require('httr')
require('jsonlite')

# BOSTON SCRAPER
# Scraped from the city's ArcGIS REST API
# https://services.arcgis.com/sFnw0xNflSi8J0uh/arcgis/rest/services/

bos_queries <- function (geo) {
  base_url <- "https://services.arcgis.com/sFnw0xNflSi8J0uh/arcgis/rest/services/Parcels19WMFull/FeatureServer/0/query?"
  format <- "geojson"
  bbox <- st_bbox(geo)
  query <- sprintf('%sf=%s&returnGeometry=true&spatialRel=esriSpatialRelIntersects&geometry={"xmin":%s,"ymin":%s,"xmax":%s,"ymax":%s,"spatialReference":{"wkid":102100}}&geometryType=esriGeometryEnvelope&inSR=102100&outFields=*&outSR=102100', base_url, format, bbox['xmin'], bbox['ymin'], bbox['xmax'], bbox['ymax'])
}

fetch_boston <- function(grid_dims, out_path){
  print("Generating City of Boston grid...")
  bos_url <- 'http://gis.cityofboston.gov/arcgis/rest/services/Planning/OpenData/MapServer/9/query?where=1%3D1&outFields=OBJECTID&outSR=102100&f=json'
  bos_bound <- st_read(bos_url) %>%
    select(c('geometry')) %>%
    st_make_grid(grid_dims, square=TRUE)
  
  print("Downloading parcels...")
  bos_parcels <- map(bos_bound, bos_queries) %>%
    lapply(function(url) {
      print(url)
      print(sprintf("Querying %s", url))
      Sys.sleep(10)
      box_return <- st_read(url)
      return(box_return)
    })
  
  print("Row-binding query results, and removing duplicates...")
  bos_parcels %>% 
    do.call(rbind, .) %>%
    distinct(FID, .keep_all = TRUE) %>%
    st_write(out_path, delete_dsn=TRUE)
  print("Finished downloading Boston parcels.")
}


# BROOKLINE SCRAPER
# Scraped from https://www.mapsonline.net/brooklinema/
# Unclear when data were last updated but parcels work with FY 2020 assessor's data.
# Possible that this ArcGIS online platform is more regularly updated
# https://www.arcgis.com/home/webmap/viewer.html?webmap=266d79a10c3e47a693742e0513e798d6

fetch_brookline <- function(out_path){
  params <- list(request = "identify_multi", 
                 sitelayers = "125194", 
                 layers = "22000", 
                 site_id = "3347",
                 results = "all",
                 mode = "replace",
                 format="json",
                 bbox="742686.562927,2938017.783368,768292.651324,2956370.726783",
                 result_id="b11065c073cccc921d9ea9ff24c6c421",
                 x1="768292.651324",
                 y1="2956370.726783",
                 x2="742686.562927",
                 y2="2938017.783368")
  print("Querying server...")
  response <- GET("https://www.mapsonline.net/peopleforms/mo4/mo4_server.php", query=params)
  print("Parsing response and writing...")
  df <-fromJSON(content(response, as="text", type="application/json"))
  df$results$results[[1]] %>%
    unnest_wider(values) %>%
    rename(
      pid = ...1,
      add = ...2,
      own = ...3
    ) %>%
    st_as_sf(wkt='geom', crs=3857) %>%
    st_write(out_path)
    print("Finished downloading Brookline parcels.")
}

fetch_brookline(out_path='brookline_parcels.gpkg')
fetch_boston(grid_dims=c(50,50), out_path='boston_parcels_fy19.gpkg')
