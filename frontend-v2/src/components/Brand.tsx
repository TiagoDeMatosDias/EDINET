export const BRAND_MARK_URL = '/brand-assets/shade-mark.svg'

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
        <span className="brand-lockup__name">
          <strong className="brand-lockup__shade">Shade</strong>
          <strong className="brand-lockup__research">Research</strong>
        </span>
        {showTagline && <small className="brand-lockup__tagline">Value in context.</small>}
      </span>
    </span>
  )
}
