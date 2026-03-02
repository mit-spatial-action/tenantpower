<script lang="ts">
    import { fly } from "svelte/transition";
    let { title = null, onclose, children } = $props();
</script>

<div class="modal-card" transition:fly={{ y: 600, duration: 800 }}>
    {#if title}
        <header class="modal-card-head has-background-primary">
            <h1 class="modal-card-title title is-3 has-text-white">
                {title}
            </h1>
        </header>
    {/if}
    <section class="modal-card-body">
        {@render children?.()}
    </section>
    <footer class="modal-card-foot">
        <button class="button is-primary is-fullwidth" onclick={onclose}
            >Continue→</button
        >
    </footer>
</div>