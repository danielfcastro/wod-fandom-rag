
import React, { useState } from 'react'
import { adminListLow, adminApprove, adminDelete, adminUpdate } from '../api.js'

export default function AdminPanel({ baseUrl }) {
  const [token, setToken] = useState('')
  const [items, setItems] = useState([])
  const [limit, setLimit] = useState(50)

  const load = async () => {
    const res = await adminListLow(token, limit)
    setItems(res.items || [])
  }
  return (
    <div className="card space-y-3">
      <h2 className="text-xl font-semibold">Admin — Revisão de Arestas (confidence: low)</h2>
      <div className="flex gap-2">
        <input className="input" placeholder="X-Admin-Token" value={token} onChange={e=>setToken(e.target.value)} />
        <input className="input" style={{maxWidth:120}} type="number" value={limit} onChange={e=>setLimit(parseInt(e.target.value||'50'))} />
        <button className="btn" onClick={load}>Carregar</button>
      </div>
      <div className="space-y-3">
        {items.map((it,i)=>(
          <AdminItem key={i} it={it} token={token} onAfter={load} />
        ))}
      </div>
    </div>
  )
}

function AdminItem({ it, token, onAfter }) {
  const [newRel,setNewRel]=React.useState('')
  const [newDst,setNewDst]=React.useState('')
  const [conf,setConf]=React.useState('')
  return (
    <div className="card">
      <div className="text-sm text-slate-400">[{it.confidence}] {it.src} -[{it.rel}]-&gt; {it.dst}</div>
      <div className="text-sm mt-1"><strong>Evidências</strong>: {JSON.stringify(it.evidence)}</div>
      <div className="flex gap-2 mt-2">
        <button className="btn" onClick={async()=>{await adminApprove(token,it.src,it.rel,it.dst); onAfter();}}>Aprovar</button>
        <button className="btn" onClick={async()=>{await adminDelete(token,it.src,it.rel,it.dst); onAfter();}}>Excluir</button>
      </div>
      <div className="mt-2 grid grid-cols-1 md:grid-cols-5 gap-2">
        <input className="input" placeholder="Novo rel (opcional)" value={newRel} onChange={e=>setNewRel(e.target.value)} />
        <input className="input" placeholder="Novo dst id (opcional)" value={newDst} onChange={e=>setNewDst(e.target.value)} />
        <input className="input" placeholder="confidence (ex.: high)" value={conf} onChange={e=>setConf(e.target.value)} />
        <button className="btn md:col-span-2" onClick={async()=>{await adminUpdate(token,it.src,it.rel,it.dst,newRel||null,newDst||null,conf||null); onAfter();}}>Atualizar</button>
      </div>
    </div>
  )
}
