import React from 'react';

export function Button({ children, onClick }) {
  return (
    <button
      style={{
        padding: '8px 16px',
        backgroundColor: '#007bff',
        color: '#fff',
        border: 'none',
        borderRadius: '4px',
        cursor: 'pointer',
      }}
      onClick={onClick}
    >
      {children}
    </button>
  );
}
