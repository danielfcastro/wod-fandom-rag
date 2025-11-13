import axios from 'axios'
const baseURL = import.meta.env.VITE_QA_BASE_URL || 'http://localhost:8000'
const api = axios.create({ baseURL })

export const askQA = async (query, top_k = 5, use_graph = true) =>
  (await api.get('/qa', { params: { query, top_k, use_graph } })).data

export const queryGraph = async (cypher) =>
  (await api.get('/graph', { params: { query: cypher } })).data

export const adminListLow = async (token, limit=50) =>
  (await api.get('/admin/edges/low', {
    params: { limit },
    headers: { 'X-Admin-Token': token }
  })).data

export const adminApprove = async (token, src, rel, dst) =>
  (await api.post('/admin/edges/approve', null, {
    params:{src,rel,dst},
    headers:{'X-Admin-Token': token}
  })).data

export const adminDelete = async (token, src, rel, dst) =>
  (await api.post('/admin/edges/delete', null, {
    params:{src,rel,dst},
    headers:{'X-Admin-Token': token}
  })).data

export const adminUpdate = async (token, src, rel, dst, new_rel, new_dst, confidence) =>
  (await api.post('/admin/edges/update', null, {
    params:{src,rel,dst,new_rel,new_dst,confidence},
    headers:{'X-Admin-Token': token}
  })).data
