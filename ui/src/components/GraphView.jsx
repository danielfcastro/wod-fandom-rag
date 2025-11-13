// ui/src/components/GraphView.jsx
import { useState } from 'react'

export default function GraphView({ onExecute, rows }) {
  const [cypher, setCypher] = useState(
    `MATCH (c:Entity {type:"Clan"})-[:REL {rel:"HAS_DISCIPLINE"}]->(d:Entity {type:"Discipline"})
RETURN c.id AS clan, collect(d.id)[0..5] AS disciplines
LIMIT 10`
  )

  const handleSubmit = (e) => {
    e.preventDefault()
    if (!cypher.trim()) return
    onExecute(cypher.trim())
  }

  const hasRows = rows && rows.length > 0
  const columns = hasRows ? Object.keys(rows[0]) : []

  return (
    <div className="p-4 bg-slate-900 rounded-xl flex flex-col gap-3">
      <form onSubmit={handleSubmit} className="flex flex-col gap-2">
        <textarea
          className="w-full min-h-[120px] px-3 py-2 rounded-lg bg-slate-800 text-slate-100 border border-slate-700 font-mono text-xs"
          value={cypher}
          onChange={e => setCypher(e.target.value)}
        />
        <button
          type="submit"
          className="self-start px-4 py-2 rounded-lg bg-indigo-500 hover:bg-indigo-400 text-slate-50 text-sm font-semibold"
        >
          Executar
        </button>
      </form>

      {hasRows && (
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm text-left border-collapse">
            <thead>
              <tr className="bg-slate-800">
                {columns.map(col => (
                  <th key={col} className="px-3 py-2 border-b border-slate-700 text-slate-200">
                    {col}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row, idx) => (
                <tr key={idx} className="odd:bg-slate-900 even:bg-slate-950">
                  {columns.map(col => (
                    <td key={col} className="px-3 py-2 border-b border-slate-800 text-slate-100">
                      {Array.isArray(row[col]) ? row[col].join(', ') : String(row[col])}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {!hasRows && (
        <p className="text-xs text-slate-400">
          Nenhum resultado ainda. Escreva uma query Cypher e clique em Executar.
        </p>
      )}
    </div>
  )
}
