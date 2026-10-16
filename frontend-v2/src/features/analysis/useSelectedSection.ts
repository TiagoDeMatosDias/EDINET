import { useEffect, useState } from 'react'

/** The areas inside a section that scroll on their own; analysis.css keeps them still until it is selected. */
const SCROLL_AREAS = '.statement-table-wrap, .chat-list'

/**
 * The section the reader last clicked in. Its scroll areas (the statement table, the discussion) take
 * the wheel; everywhere else the wheel scrolls the page, so it never sticks on its way past them.
 * Clicking outside the section, moving focus out of it, or Escape releases it. Sections are the
 * page's ``.analysis-section`` elements, known by their ids; each shows it is selected with ``data-selected``.
 */
export function useSelectedSection<Id extends string>(ready: boolean) {
  const [selected, setSelected] = useState<Id | null>(null)
  useEffect(() => {
    if (!ready) return
    const sectionOf = (target: EventTarget | null) => (target instanceof Element ? target.closest('.analysis-section')?.id as Id | undefined : undefined) || null
    // A click rather than a press, so a touch that starts on the table and scrolls still scrolls the page.
    const onClick = (event: MouseEvent) => setSelected(sectionOf(event.target))
    const onFocusIn = (event: FocusEvent) => {
      const section = sectionOf(event.target)
      // Tabbing or arrowing into a scroll area selects its section; a jump to a heading does not.
      if (event.target instanceof Element && event.target.closest(SCROLL_AREAS)) setSelected(section)
      else setSelected(current => current === section ? current : null)
    }
    const onKeyDown = (event: KeyboardEvent) => { if (event.key === 'Escape') setSelected(null) }
    document.addEventListener('click', onClick, true)
    document.addEventListener('focusin', onFocusIn)
    document.addEventListener('keydown', onKeyDown)
    return () => {
      document.removeEventListener('click', onClick, true)
      document.removeEventListener('focusin', onFocusIn)
      document.removeEventListener('keydown', onKeyDown)
    }
  }, [ready])
  return selected
}
