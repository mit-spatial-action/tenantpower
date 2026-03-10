<script lang="ts">
    import mapboxgl from "mapbox-gl";
    import MapboxGeocoder from "@mapbox/mapbox-gl-geocoder";
    import type { FeatureCollection } from 'geojson';
    import type { GeoJSONSource } from 'mapbox-gl';
    import { onMount, onDestroy } from "svelte";
    
    import "mapbox-gl/dist/mapbox-gl.css";
    import "@mapbox/mapbox-gl-geocoder/dist/mapbox-gl-geocoder.css";

    import { PUBLIC_MB_TOKEN } from '$env/static/public';
    import { appState } from '$lib/state.svelte';
    import { goto } from "$app/navigation";

    let map: mapboxgl.Map | undefined;
    let mapContainer: HTMLDivElement;

    interface MapState {
        zoom: number;
        style: string;
        bounds: mapboxgl.LngLatBoundsLike;
    }

    const bounds = new mapboxgl.LngLatBounds(
        [-71.1912506475513, 42.22791953938691],
        [-70.98634143082624, 42.45345337436338],
    );
    const boundsNorm = mapboxgl.LngLatBounds.convert(bounds);

    const boundsArray = boundsNorm.toArray();

    const bbox: [number, number, number, number] = [
        boundsArray[0][0],
        boundsArray[0][1],
        boundsArray[1][0],
        boundsArray[1][1],
    ];
    const xMargin = Math.abs(boundsNorm.getWest() - boundsNorm.getEast()) * 0.5;
    const yMargin =
        Math.abs(boundsNorm.getNorth() - boundsNorm.getSouth()) * 0.5;

    const initialState: MapState = {
        zoom: 9,
        style: "mapbox://styles/mit-spatial-action/cmd4tea3k009701s25hl6hr5y",
        bounds: bounds,
    };

    $effect(() => {
        const data = $state.snapshot(appState.selected) as FeatureCollection;
        if (map && data && map.isStyleLoaded()) {
            // map?.flyTo({
            //     center: [
            //         data.features[0].properties?.lon,
            //         data.features[0].properties?.lat
            //     ],
            //     zoom: 18
            // })
            const source = map.getSource('parcels-fill-source') as GeoJSONSource;
            if (source) {
                console.log(data);
                source.setData(data);
            }
        }
    });

    onMount(() => {
        map = new mapboxgl.Map({
            container: mapContainer,
            accessToken: PUBLIC_MB_TOKEN,
            style: initialState.style,
            bounds: initialState.bounds,
            maxBounds: new mapboxgl.LngLatBounds(
                [
                    boundsNorm.getWest() - xMargin,
                    boundsNorm.getSouth() - yMargin,
                ],
                [
                    boundsNorm.getEast() + xMargin,
                    boundsNorm.getNorth() + yMargin,
                ],
            ),
            maxZoom: 21,
            minZoom: 8,
            pitchWithRotate: false,
            dragRotate: false,
        });

        const geocoder = new MapboxGeocoder({
            accessToken: PUBLIC_MB_TOKEN,
            useBrowserFocus: true,
            mapboxgl: mapboxgl as any,
            types: "address",
            countries: "us",
            bbox: bbox,
            flyTo: false,
            marker: false,
            filter: (item) => {
                return item.context.some((i) => {
                    return (
                        i.id.split(".").shift() === "region" &&
                        i.text === "Massachusetts"
                    );
                });
            },
        });

        map.addControl(geocoder, "top-left");

        map.on("load", () => {
            if (map) {
                map.addSource("bounds-source", {
                    type: "geojson",
                    data: "/data/bounds.geojson",
                });

                map.addLayer({
                    id: "bounds",
                    type: "line",
                    source: "bounds-source",
                    paint: {
                        "line-color": "white",
                        "line-width": 2,
                        "line-blur": 0.5,
                    },
                    slot: 'middle'
                });

                map.addSource("parcels-source", {
                    type: "vector",
                    url: "mapbox://mit-spatial-action.79vtaci2",
                });

                map.addLayer({
                    id: "parcel-points",
                    type: "circle",
                    source: "parcels-source",
                    "source-layer": "tenantpower-cz585a",
                    paint: {
                        'circle-color': 'white',
                        'circle-stroke-color': 'red',
                        'circle-stroke-width': [
                            'interpolate',
                            ['linear'],
                            ['zoom'],
                            13, 0,
                            22, 3
                        ],
                        'circle-opacity': 1,
                        'circle-radius': [
                            'interpolate',
                            ['linear'],
                            ['zoom'],
                            13, 0.5,
                            22, 10
                        ]
                    },
                    slot: 'middle'
                });

                map.addInteraction('parcel-points-mouseenter', {
                    type: 'mouseenter',
                    target: { layerId: 'parcel-points' },
                    handler: () => {
                        if (map) map.getCanvas().style.cursor = (map.getZoom() > 15) ? 'pointer' : '';
                    }
                });
                map.addInteraction('parcel-points-mouseleave', {
                    type: 'mouseleave',
                    target: { layerId: 'parcel-points' },
                    handler: () => {
                        if (map) map.getCanvas().style.cursor = '';
                    }
                });

                map.addInteraction('parcel-points-click', {
                    type: 'click',
                    target: { layerId: 'parcel-points' },
                    handler: (e) => {
                        if (map) (map.getZoom() > 15) && goto(`/prop/${e.feature?.properties.id}`);
                    }
                });

            }

            geocoder.on("result", async (e) => {
                appState.loading = true;
                const coords = e.result.geometry.coordinates;
                const response = await fetch(`props_by_loc/${coords[0]}/${coords[1]}/1`);
                appState.loading = false;
                const results: FeatureCollection = await response.json();
                if (results.features && results.features.length > 0) {
                    const selected = results.features.filter(f => {
                        return f.properties?.prop_addr?.split(' ')[0] === e.result.address;
                    })
                    if (selected.length > 0) {
                        goto('prop/' + selected[0].properties?.id);
                    } else {
                        appState.loading = false;
                    }
                }
            });
        });

        map.once("idle", () => {
            appState.loading = false;
        });
    });

    onDestroy(() => {
        if (map) map.remove();
    });
</script>

<div class="map" bind:this={mapContainer}></div>

<style>
    .map {
        position: absolute;
        padding: 0;
        margin: 0;
        width: 100%;
        height: 100%;
    }
</style>
