<script lang="ts">
    import mapboxgl from 'mapbox-gl';
    import MapboxGeocoder from '@mapbox/mapbox-gl-geocoder';
    import { onMount, onDestroy } from 'svelte';

    import 'mapbox-gl/dist/mapbox-gl.css';
    import '@mapbox/mapbox-gl-geocoder/dist/mapbox-gl-geocoder.css';

    import { PUBLIC_MB_TOKEN } from '$env/static/public';
    import { appState } from '$lib/state.svelte';

    let map: mapboxgl.Map | undefined;
    let mapContainer: HTMLDivElement;

    interface MapState {
        zoom: number;
        style: string;
        bounds: mapboxgl.LngLatBoundsLike
    };

    const bounds = new mapboxgl.LngLatBounds(
        [-71.1912506475513, 42.22791953938691], 
        [-70.86884578454921, 42.45345337436338]
    )
    const boundsNorm = mapboxgl.LngLatBounds.convert(bounds);

    const boundsArray = boundsNorm.toArray()

    const bbox: [number, number, number, number] = [
        boundsArray[0][0],
        boundsArray[0][1],
        boundsArray[1][0],
        boundsArray[1][1]
    ]
    const xMargin = Math.abs(boundsNorm.getWest() - boundsNorm.getEast()) * 0.2;
    const yMargin = Math.abs(boundsNorm.getNorth() - boundsNorm.getSouth()) * 0.2;

    const initialState: MapState = {
        zoom: 9, 
        style: "mapbox://styles/mit-spatial-action/cmd4tea3k009701s25hl6hr5y" ,
        bounds: bounds
    };

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
                ], [
                    boundsNorm.getEast() + xMargin, 
                    boundsNorm.getNorth() + yMargin
                ]
            ),
            maxZoom: 21,
            minZoom: 11,
            pitchWithRotate: false,
            dragRotate: false
        });

        map.addControl(
            new MapboxGeocoder({
                accessToken: PUBLIC_MB_TOKEN,
                useBrowserFocus: true,
                mapboxgl: mapboxgl as any,
                types: 'address',
                countries: 'us',
                bbox: bbox,
                filter: (item) => {
                    return item.context.some((i) => {
                        return (
                            i.id.split('.').shift() === 'region' &&
                            i.text === 'Massachusetts'
                        );
                    });
                }
            }),
            'top-left'
        );
        
        map.on('load', () => {
            map && map.addSource('bounds-source', {
                type: 'geojson',
                data: '/data/bounds.geojson' 
            });

            map && map.addLayer({
                id: 'bounds',
                type: 'line',
                source: 'bounds-source',
                paint: {
                    'line-color': 'white',
                    'line-width': 2,
                    'line-blur': 0.5
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