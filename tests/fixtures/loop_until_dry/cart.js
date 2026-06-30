// Sanitized cart fixture for loop-until-dry parity tests.
// Deliberately tiny and credential-free: it gives fake child agents a stable
// subject without depending on the original Claude archive or live services.
export function calculateCart(items, discount) {
  const subtotal = items.reduce((sum, item) => sum + item.price * item.quantity, 0);
  const discounted = subtotal - (discount?.amount || 0);
  return {
    subtotal,
    total: Math.max(0, discounted),
    itemCount: items.length,
  };
}

export function restoreCart(snapshot) {
  return {
    items: Array.isArray(snapshot?.items) ? snapshot.items : [],
    discount: snapshot?.discount || null,
  };
}
