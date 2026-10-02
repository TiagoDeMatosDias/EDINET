import { useLayoutEffect, type RefObject } from 'react'

/**
 * Pins an open popover to its anchor with fixed positioning, so the scrolling
 * rules panel never clips it. It opens below the anchor, or above when there is
 * more room there, and stays inside the window horizontally.
 */
export function useAnchoredPopover(anchor: RefObject<HTMLElement | null>, popover: RefObject<HTMLElement | null>, open: boolean, align: 'left' | 'right' = 'left') {
  useLayoutEffect(() => {
    if (!open) return
    const place = () => {
      const box = anchor.current?.getBoundingClientRect()
      const element = popover.current
      if (!box || !element) return
      const below = window.innerHeight - box.bottom
      const above = box.top
      const upward = below < 260 && above > below
      const width = element.offsetWidth
      const left = align === 'right' ? box.right - width : box.left
      Object.assign(element.style, {
        position: 'fixed',
        top: upward ? '' : `${box.bottom + 4}px`,
        bottom: upward ? `${window.innerHeight - box.top + 4}px` : '',
        left: `${Math.max(8, Math.min(left, window.innerWidth - width - 8))}px`,
        right: 'auto',
        maxHeight: `${Math.max(160, (upward ? above : below) - 12)}px`,
      })
    }
    place()
    window.addEventListener('resize', place)
    window.addEventListener('scroll', place, true)
    return () => {
      window.removeEventListener('resize', place)
      window.removeEventListener('scroll', place, true)
    }
  }, [align, anchor, open, popover])
}
