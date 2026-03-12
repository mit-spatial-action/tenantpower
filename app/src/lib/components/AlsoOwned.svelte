<script lang="ts">
    import LabelTag from "./LabelTag.svelte";
    import { USDollar, muniExpand, selectFeature } from "$lib/helpers/helpers";
    import { appState } from "$lib/state.svelte";
    import type { Feature } from "geojson";
    let { feature }: { feature: Feature } = $props();
    const prop = $derived(feature?.properties ?? {});

    const onmouseenter = () => {
        appState.highlighted = prop.id;
    };

    const onmouseleave = () => {
        appState.highlighted = null;
    };
</script>

{#if prop.own}
    <div
        {onmouseenter}
        {onmouseleave}
        onclick={() => selectFeature(prop.id)}
        onkeydown={() => selectFeature(prop.id)}
        role="menuitem"
        tabindex="0"
        class="cell box bordered {appState.highlighted == prop.id
            ? 'highlighted'
            : ''}"
    >
        <div class="block header p-2">
            <p class="has-text-weight-bold">{prop.prop_addr}</p>
            {#if prop.town}
                {muniExpand(prop.town)}
            {/if}
        </div>
        {#if prop.count > 1}
            <div class="field is-grouped is-grouped-multiline">
                <LabelTag label={"Value"}
                    >{USDollar.format(prop.ass_val)}</LabelTag
                >
                <LabelTag label={"Sale Price"}
                    >{USDollar.format(prop.sale_p)}</LabelTag
                >
                <LabelTag label={"Sale Date"}
                    >{new Date(prop.sale_d).toLocaleDateString(
                        "en-US",
                    )}</LabelTag
                >
            </div>
        {/if}
    </div>
{/if}

<style>
    .highlighted {
        cursor: pointer;
    }
    .highlighted > .header {
        background-color: var(--bulma-primary);
        color: white;
    }
</style>
