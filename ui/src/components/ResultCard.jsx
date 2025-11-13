// ui/src/components/ResultCard.jsx
export default function ResultCard({ passage }) {
  const { title, url, text, score, section } = passage

  return (
    <div className="p-4 rounded-xl bg-slate-900 border border-slate-800 shadow-sm">
      <div className="flex items-baseline justify-between gap-2">
        <h3 className="text-lg font-semibold text-slate-100">
          {title || 'Sem título'}
        </h3>
        <span className="text-xs text-slate-400">
          score: {score?.toFixed ? score.toFixed(2) : score}
        </span>
      </div>
      {section && (
        <p className="text-xs text-slate-400 mt-1">
          Seção: {section}
        </p>
      )}
      <p className="mt-2 text-sm text-slate-200 whitespace-pre-wrap">
        {text}
      </p>
      {url && (
        <a
          href={url}
          target="_blank"
          rel="noreferrer"
          className="inline-block mt-3 text-xs text-emerald-400 hover:text-emerald-300"
        >
          Ver no Fandom →
        </a>
      )}
    </div>
  )
}
