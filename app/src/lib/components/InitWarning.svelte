<script lang="ts">
    import { fade, fly } from "svelte/transition";
    import { appState } from "$lib/state.svelte";

    let isVisible = $state(true);
    let agreed = $state(false);
    let hasAttempted = $state(false);

    const handleContinue = () => { 
        hasAttempted = true;
        if (agreed) {
            isVisible = false;
            appState.isInit = false;
        }
    };
</script>

{#if isVisible && appState.isInit }
<div class="modal is-active" transition:fade={{ duration: 200 }}>
    <div class="modal-background"></div>
    <div class="modal-card bordered" transition:fly={{ y: 600, duration: 800 }}>
        <section class="modal-card-body">
            <div class="block">
                <article class="message is-primary">
                    <div class="message-header">
                        <p>🚨 Warning! 🚨</p>
                    </div>
                    <div class="message-body">
                        <div class="block">
                            The Tenant Power database has not been updated since
                            2021. We are continuing to make the tool available
                            for archival purposes, but information gleaned from
                            this platform should be treated with <strong
                                >extreme</strong
                            > caution.
                        </div>
                        <div class="block">
                            <label class="checkbox">
                                <div class="control">
                                    <input
                                        type="checkbox"
                                        bind:checked={agreed}
                                    />
                                    I understand that the tool should not be treated as current or authoritative!
                                </div>
                                {#if !agreed && hasAttempted}
                                    <p class="help is-primary">
                                        We require that you agree!
                                    </p>
                                {/if}
                            </label>
                        </div>
                    </div>
                </article>
            </div>
            <div class="block">
                Tenant Power used city assessor's data from
                <a href="https://www.cityofboston.gov/assessing/search/"
                    >Boston</a
                >,
                <a href="https://www.cambridgema.gov/PropertyDatabase"
                    >Cambridge</a
                >,
                <a href="http://gis.vgsi.com/SomervilleMA/Search.aspx"
                    >Somerville</a
                >,
                <a href="http://gis.vgsi.com/medfordma/">Medford</a>, and
                <a
                    href="http://apps.brooklinema.gov/assessors/propertylookup.asp"
                    >Brookline</a
                >, Massachusetts to identify institutional owners, and the
                proprties they own, using machine learning. It was able to make
                pretty good guesses! But we encourage you to check
                <a href="http://www.masslandrecords.com/"
                    >Massachusetts Land Records</a
                >
                and/or the assessors databases linked above to confirm ownership.
            </div>
            <div class="block">
                <a href="https://ericrobskyhuntley.github.io"
                    >Eric Robsky Huntley</a
                >
                of the
                <a href="mit-spatial-action.github.io"
                    >MIT Spatial Action & Analysis Research Group</a
                >
                built Tenant Power as part of the
                <a href="https://mutualaidmamas.com/"
                    >Mutual Aid Medford and Somerville (MAMAS)</a
                > Housing Justice Working Group.
            </div>
        </section>
        <footer class="modal-card-foot">
            <button class="button is-primary is-fullwidth" onclick={handleContinue} disabled={!agreed}
                >Continue→</button
            >
        </footer>
    </div>
</div>
{/if}