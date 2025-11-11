
import React, { useState } from 'react'
import SearchBar from './components/SearchBar.jsx'
import ResultCard from './components/ResultCard.jsx'
import GraphView from './components/GraphView.jsx'
import AdminPanel from './components/AdminPanel.jsx'
import { askQA, queryGraph } from './api.js'

export default function App() {
  const [q,setQ]=useState('')
  const [loading,setLoading]=useState(false)
  const [answers,setAnswers]=useState([])
  const [graph,setGraph]=useState([])
  const [cypher,setCypher]=useState('MATCH (c:Entity {type:"Clan"})-[:REL {rel:"HAS_DISCIPLINE"}]->(d:Entity {type:"Discipline"}) RETURN c.name AS clan, collect(d.name)[0..5] AS sample LIMIT 10')
  const [rows,setRows]=useState([])

  const run = async ()=>{ setLoading(true); try{ const r=await askQA(q,6,true); setAnswers(r.answers||[]); setGraph(r.graph||[]) } finally{ setLoading(false) } }
  const runCypher = async ()=>{ const r=await queryGraph(cypher); setRows(r.rows||[]) }

  return (
    <div className="max-w-5xl mx-auto p-6 space-y-6">
      <header className="space-y-2">
        <h1 className="text-3xl font-bold">World of Darkness — RAG UI</h1>
        <p className="text-slate-400">Busca híbrida + re-ranker + grafo leve (Fandom)</p>
      </header>

      <section className="space-y-3">
        <SearchBar value={q} onChange={setQ} onSubmit={run} loading={loading} />
        {graph?.length>0 && <GraphView data={graph} />}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {answers.map((a,i)=><ResultCard key={i} item={a} />)}
        </div>
      </section>

      <section className="space-y-3">
        <h2 className="text-xl font-semibold">Consulta ao Grafo (Cypher, read-only)</h2>
        <textarea className="input h-28" value={cypher} onChange={e=>setCypher(e.target.value)} />
        <button className="btn" onClick={runCypher}>Executar</button>
        {rows.length>0 && (
          <div className="card overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead className="text-slate-400">
                <tr>{Object.keys(rows[0]||{}).map(h=><th key={h} className="text-left pr-4 pb-2">{h}</th>)}</tr>
              </thead>
              <tbody>
                {rows.map((r,i)=>(
                  <tr key={i}>{Object.keys(rows[0]||{}).map(h=><td key={h} className="pr-4 py-1">{JSON.stringify(r[h])}</td>)}</tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <section className="space-y-3">
        <details className="card">
          <summary className="cursor-pointer">Admin (revisão de arestas)</summary>
          <div className="mt-3">
            <AdminPanel />
          </div>
        </details>
      </section>

      <footer className="text-slate-500 text-sm">
        <p>Licenças: Fandom (CC BY-SA). Projeto educacional / protótipo.</p>
      </footer>
    </div>
  )
}
