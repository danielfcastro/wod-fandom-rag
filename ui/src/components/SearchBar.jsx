// ui/src/components/SearchBar.jsx
import { useState } from 'react'

export default function SearchBar({ onSearch }) {
  const [query, setQuery] = useState('')
  const [topK, setTopK] = useState(6)
  const [useGraph, setUseGraph] = useState(true)

  const handleSubmit = (e) => {
    e.preventDefault()
    if (!query.trim()) return
    onSearch(query.trim(), topK, useGraph)
  }

  return (
    <form onSubmit={handleSubmit} className="flex flex-col gap-2 p-4 bg-slate-900 rounded-xl">
      <div className="flex gap-2">
        <input
          className="flex-1 px-3 py-2 rounded-lg bg-slate-800 text-slate-100 border border-slate-700"
          placeholder="Faça uma pergunta sobre o universo de World of Darkness..."
          value={query}
          onChange={e => setQuery(e.target.value)}
        />
        <button
          type="submit"
          className="px-4 py-2 rounded-lg bg-emerald-500 hover:bg-emerald-400 text-slate-900 font-semibold"
        >
          Buscar
        </button>
      </div>
      <div className="flex items-center gap-4 text-sm text-slate-300">
        <label className="flex items-center gap-2">
          Top K:
          <input
            type="number"
            min={1}
            max={50}
            value={topK}
            onChange={e => setTopK(Number(e.target.value) || 1)}
            className="w-16 px-2 py-1 rounded bg-slate-800 border border-slate-700"
          />
        </label>
        <label className="flex items-center gap-2">
          <input
            type="checkbox"
            checked={useGraph}
            onChange={e => setUseGraph(e.target.checked)}
          />
          Usar grafo (Neo4j)
        </label>
      </div>
    </form>
  )
}
