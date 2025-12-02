// src/config/api.js
export const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:3001';

export const API_ENDPOINTS = {
  VISITORS: `${API_BASE_URL}/api/visitors/list`,
  DEVICES: `${API_BASE_URL}/api/displayforce/device/list`,
  // ... outros endpoints
};