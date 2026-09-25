// Bump the version whenever the mark changes: brand assets keep stable paths and browsers cache them hard.
export const BRAND_MARK_URL = '/brand-assets/shade-mark.svg?v=2'

interface BrandLockupProps {
  className?: string
  compact?: boolean
  showTagline?: boolean
  tone?: 'light' | 'dark'
}

export function BrandLockup({
  className = '',
  compact = false,
  showTagline = false,
  tone = 'light',
}: BrandLockupProps) {
  const classes = [
    'brand-lockup',
    `brand-lockup--${tone}`,
    compact ? 'brand-lockup--compact' : '',
    className,
  ].filter(Boolean).join(' ')

  return (
    <span
      className={classes}
      role="img"
      aria-label={`Shade Research${showTagline ? '. Value in context.' : ''}`}
    >
      <img className="brand-lockup__mark" src={BRAND_MARK_URL} alt="" aria-hidden="true" />
      <span className="brand-lockup__copy" aria-hidden="true">
        <span className="brand-lockup__name">Shade Research</span>
        {showTagline && <small className="brand-lockup__tagline">Value in context</small>}
      </span>
    </span>
  )
}
