import { useEffect, useState } from 'react'

/**
 * Whether an element's content is taller than the element: it scrolls, or would if allowed to.
 * Give the returned ref to the element; its first child is watched too, as that is what grows.
 */
export function useClipped<T extends HTMLElement>() {
  const [element, setElement] = useState<T | null>(null)
  const [clipped, setClipped] = useState(false)
  useEffect(() => {
    if (!element || typeof ResizeObserver === 'undefined') return
    const observer = new ResizeObserver(() => setClipped(element.scrollHeight > element.clientHeight + 1))
    observer.observe(element)
    if (element.firstElementChild) observer.observe(element.firstElementChild)
    return () => observer.disconnect()
  }, [element])
  return [setElement, clipped] as const
}
