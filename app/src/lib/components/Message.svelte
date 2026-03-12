<script lang="ts">
    let { header = null, children } = $props();
</script>

<article class="message is-primary">
    {#if header}
        <div class="message-header">
            <p>{header}</p>
        </div>
    {/if}
    <div class="message-body">
        {@render children?.()}
    </div>
</article>