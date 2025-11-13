// ui/src/components/GraphView.jsx
import { useState } from "react";
import { queryGraph } from "../api";

export default function GraphView() {
  const [cypher, setCypher] = useState(
    'MATCH (c:Entity {type:"Clan"})-[:REL {rel:"HAS_DISCIPLINE"}]->(d:Entity {type:"Discipline"}) RETURN c.name AS clan, collect(d.name)[0..5] AS sample LIMIT 10'
  );
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");

  const onRun = async () => {
    setLoading(true);
    setErr("");
    try {
      const data = await queryGraph(cypher);
      setRows(data.rows || []);
    } catch (e) {
      setErr(String(e));
      setRows([]);
    } finally {
      setLoading(false);
    }
  };

  const keys = rows.length ? Object.keys(rows[0]) : [];

  return (
    <div className="p-4 space-y-3">
      <h2 className="text-xl font-semibold">Consulta ao Grafo (Cypher, read-only)</h2>
      <textarea
        className="w-full border rounded p-2 font-mono text-sm"
        rows={5}
        value={cypher}
        onChange={(e) => setCypher(e.target.value)}
      />
      <button
        className="px-4 py-2 rounded bg-black text-white disabled:opacity-60"
        onClick={onRun}
        disabled={loading}
      >
        {loading ? "Executando..." : "Executar"}
      </button>
      {err && <div className="text-red-600 text-sm">{err}</div>}

      <div className="overflow-auto border rounded">
        <table className="min-w-full text-sm">
          <thead>
            <tr className="bg-gray-100">
              {keys.map((k) => (
                <th key={k} className="text-left p-2 border-b">{k}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => (
              <tr key={i} className="odd:bg-white even:bg-gray-50">
                {keys.map((k) => (
                  <td key={k} className="p-2 border-b">
                    {Array.isArray(r[k]) ? r[k].join(", ") : String(r[k])}
                  </td>
                ))}
              </tr>
            ))}
            {!rows.length && (
              <tr><td className="p-3 text-gray-400">Sem resultados</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
