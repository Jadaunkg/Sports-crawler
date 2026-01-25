-- Migration: Add site_type and sport_focus columns to sites table
-- Run this in Supabase SQL Editor to update existing database

-- Add site_type column (default 'general')
ALTER TABLE sites 
ADD COLUMN IF NOT EXISTS site_type TEXT DEFAULT 'general';

-- Add sport_focus column (nullable, for specific sport sites)
ALTER TABLE sites 
ADD COLUMN IF NOT EXISTS sport_focus TEXT;

-- Update existing sites to have appropriate defaults if needed
-- Uncomment and modify if you want to set existing sites to specific types:
-- UPDATE sites SET site_type = 'specific', sport_focus = 'soccer' WHERE domain = 'goal.com';
-- UPDATE sites SET site_type = 'specific', sport_focus = 'soccer' WHERE domain = 'therealchamps.com';
