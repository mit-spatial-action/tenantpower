<script>
    import { appState } from "$lib/state.svelte";
    import { slide } from 'svelte/transition';
    
    const clearError = () => {
        appState.errors.geocode = false;
    }
</script>

<div class="box">
    <div id="geocoder" class="block"></div>
    {#if appState.errors.geocode}
    <div class="block" transition:slide={{duration:100, axis:'y'}}>
        <article class="message is-primary">
            <div class="message-header">
                <p>No Records Found</p>
                <button class="delete" aria-label="delete" onclick={clearError}></button>
            </div>
            <div class="message-body">
                This could be because data are missing/outdated or because 
                the address is either a non-residential property or outside 
                the application's service area.
            </div>
        </article>
    </div>
    {/if}
</div>

<style>
    :global(.mapboxgl-ctrl-geocoder) {
        font-family: var(--bulma-body-family) !important;
        width: 100% !important;
        border-radius: 0;
        max-width: none !important;
    }
</style>