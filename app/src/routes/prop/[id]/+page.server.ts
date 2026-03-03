import type { PageServerLoad } from './$types';
import { API_KEY } from '$env/static/private';

export const load: PageServerLoad = async ({ params, fetch }) => {
    const { id } = params;
    const response = await fetch(`/api/props/${id}`, {
        headers: {
            'X-API-Key': `${API_KEY}`
        }
    });

    if (!response.ok) return null;
    return await response.json();
};