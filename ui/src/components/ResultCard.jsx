
import React from 'react'
export default function ResultCard({ item }) {
  return (
    <div className="card space-y-2">
      <div className="text-sm text-slate-400">{item.title} — <span className="italic">{item.section}</span></div>
      <div className="whitespace-pre-wrap leading-relaxed">{item.text}</div>
      <div className="text-sm text-slate-400">
        Score: {item.score?.toFixed?.(3) ?? '–'} • <a className="link" href={item.url} target="_blank" rel="noreferrer">Fonte</a>
      </div>
    </div>
  )
}
