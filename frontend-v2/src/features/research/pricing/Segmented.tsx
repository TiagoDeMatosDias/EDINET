/** A small either/or switch of pressed buttons; safe inside a field's label, unlike radio inputs. */
export function Segmented<T extends string>({ label, value, options, onChange }: {
  label: string
  value: T
  options: ReadonlyArray<{ value: T; label: string; title?: string }>
  onChange: (value: T) => void
}) {
  return <span className="rs-segmented" role="group" aria-label={label}>
    {options.map(option => <button key={option.value} type="button" title={option.title} aria-pressed={value === option.value} onClick={() => onChange(option.value)}>{option.label}</button>)}
  </span>
}
