// ui/src/App.jsx
import { useState } from 'react'
import { askQA, queryGraph } from './api'
import SearchBar from './components/SearchBar'
import ResultCard from './components/ResultCard'
import GraphView from './components/GraphView'
import './styles.css'

function App() {
  const [lastQuery, setLastQuery] = useState('')
  const [results, setResults] = useState([])
  const [graphRows, setGraphRows] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  const handleSearch = async (query, top_k = 6, use_graph = true) => {
    setLoading(true)
    setError(null)
    try {
      const data = await askQA(query, top_k, use_graph)
      setLastQuery(data.query || query)
      setResults(data.passages || [])
      setGraphRows(use_graph ? (data.graph || []) : [])
    } catch (e) {
      console.error(e)
      setError(e.message || 'Erro ao buscar')
    } finally {
      setLoading(false)
    }
  }

  const handleGraphExecute = async (cypher) => {
    setError(null)
    try {
      const data = await queryGraph(cypher)
      setGraphRows(data.rows || [])
    } catch (e) {
      console.error(e)
      setError(e.message || 'Erro ao consultar o grafo')
    }
  }

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100">
      <div className="max-w-6xl mx-auto py-6 px-4 flex flex-col gap-4">
        <header className="flex items-center justify-between gap-4">
          <div>
            <h1 className="text-2xl font-bold">WoD Fandom RAG</h1>
            <p className="text-sm text-slate-400">
              Pergunte sobre World of Darkness, com busca híbrida + grafo Neo4j.
            </p>
          </div>
        </header>

        <SearchBar onSearch={handleSearch} />

        {error && (
          <div className="p-3 rounded-lg bg-red-900/40 border border-red-700 text-sm">
            {error}
          </div>
        )}

        {loading && (
          <div className="text-sm text-slate-300">
            Buscando resultados...
          </div>
        )}

        {lastQuery && (
          <p className="text-xs text-slate-400">
            Última pergunta: <span className="text-slate-200">{lastQuery}</span>
          </p>
        )}

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 items-start">
          <section className="flex flex-col gap-3">
            <h2 className="text-sm font-semibold text-slate-300 uppercase tracking-wide">
              Passagens encontradas
            </h2>
            {results.length === 0 && !loading && (
              <p className="text-sm text-slate-500">
                Nenhum resultado ainda. Faça uma pergunta acima.
              </p>
            )}
            {results.map((p, idx) => (
              <ResultCard key={idx} passage={p} />
            ))}
          </section>

          <section className="flex flex-col gap-3">
            <h2 className="text-sm font-semibold text-slate-300 uppercase tracking-wide">
              Consulta ao grafo (Cypher, read-only)
            </h2>
            <GraphView onExecute={handleGraphExecute} rows={graphRows} />
          </section>
        </div>
      </div>
    </div>
  )
}

export default App
