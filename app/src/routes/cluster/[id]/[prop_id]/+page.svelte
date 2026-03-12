<script lang="ts">
    import { appState } from "$lib/state.svelte";
    import PropertyCard from "$lib/components/PropertyCard.svelte";
    import AlsoOwned from "$lib/components/AlsoOwned.svelte";
    import type { FeatureCollection } from "geojson";
    import type { PageProps } from "./$types";

    let { data }: PageProps & { data: FeatureCollection } = $props();

    $effect(() => {
        if (data) {
            appState.selected = data;
        }
    });
</script>

{#if appState.selected?.features?.length}
    <PropertyCard feature={appState.selected.features[0]} />
    <div class="grid mx-2 is-col-min-12">
        {#each appState.selected.features.slice(1) as feature (feature.id)}
            <AlsoOwned {feature} />
        {/each}
    </div>
{/if}