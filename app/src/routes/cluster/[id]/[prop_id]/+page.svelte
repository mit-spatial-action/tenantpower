<script lang="ts">
    import { appState } from "$lib/state.svelte";
    import PropertyCard from "$lib/components/PropertyCard.svelte";
    import type { FeatureCollection } from "geojson";
    import type { PageProps } from "./$types";

    let { data }: PageProps & { data: FeatureCollection } = $props();

    $effect(() => {
        if (data) {
            appState.selected = data;
        }
    });
</script>

{#if data?.features?.length}
    <PropertyCard feature={data.features[0]} />
{/if}