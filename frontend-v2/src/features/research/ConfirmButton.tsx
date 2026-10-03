import { useEffect, useState, type ReactNode } from 'react'

/** A destructive action that asks once more: the first click arms it for a few seconds, the second acts. */
export function ConfirmButton({ label, confirmLabel = 'Confirm', onConfirm, className = 'text-button text-button--danger', armed: armedProp, onArmedChange, children }: {
  label: string
  confirmLabel?: string
  onConfirm: () => void
  className?: string
  /** Lets a keyboard shortcut arm the button from outside. */
  armed?: boolean
  onArmedChange?: (armed: boolean) => void
  children?: ReactNode
}) {
  const [armedState, setArmedState] = useState(false)
  const armed = armedProp ?? armedState
  const setArmed = (next: boolean) => { setArmedState(next); onArmedChange?.(next) }
  useEffect(() => {
    if (!armed) return
    const timer = window.setTimeout(() => { setArmedState(false); onArmedChange?.(false) }, 4000)
    return () => window.clearTimeout(timer)
  }, [armed, onArmedChange])
  return <button
    type="button"
    className={armed ? `${className} is-armed` : className}
    aria-label={armed ? `${confirmLabel}: ${label}` : label}
    onClick={() => { if (armed) { setArmed(false); onConfirm() } else setArmed(true) }}
    onBlur={() => { if (armed && armedProp === undefined) setArmed(false) }}
  >{armed ? confirmLabel : children ?? label}</button>
}
