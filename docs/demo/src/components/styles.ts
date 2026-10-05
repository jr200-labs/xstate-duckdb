import { cn } from '../utils'

import { DisplayOutputResult } from './types'

export const getButtonClasses = (type: DisplayOutputResult['type'], disabled: boolean = false) => {
  const baseClasses = 'px-3 py-1.5 text-white rounded-md transition-colors text-sm'
  const styles = getTypeStyles(type)

  return cn(baseClasses, disabled ? styles.disabled : cn(styles.base, styles.hover))
}

export const getTypeStyles = (type: DisplayOutputResult['type']) => {
  switch (type) {
    case 'error':
      return {
        base: 'bg-red-500',
        hover: 'hover:bg-red-600',
        disabled: 'bg-gray-400 text-gray-200 cursor-not-allowed',
      }
    case 'configure':
      return {
        base: 'bg-blue-500',
        hover: 'hover:bg-blue-600',
        disabled: 'bg-gray-400 text-gray-200 cursor-not-allowed',
      }
    case 'connect':
      return {
        base: 'bg-green-500',
        hover: 'hover:bg-green-600',
        disabled: 'bg-gray-400 text-gray-200 cursor-not-allowed',
      }
    case 'disconnect':
      return {
        base: 'bg-red-500',
        hover: 'hover:bg-red-600',
        disabled: 'bg-gray-400 text-gray-200 cursor-not-allowed',
      }
    case 'reset':
      return {
        base: 'bg-yellow-500',
        hover: 'hover:bg-yellow-600',
        disabled: 'bg-gray-400 text-gray-200 cursor-not-allowed',
      }
    case 'query.execute':
      return {
        base: 'bg-purple-500',
        hover: 'hover:bg-purple-600',
        disabled: 'bg-gray-400 text-gray-200 cursor-not-allowed',
      }
    case 'transaction.begin':
      return {
        base: 'bg-teal-500',
        hover: 'hover:bg-teal-600',
        disabled: 'bg-gray-400 text-gray-200 cursor-not-allowed',
      }
    case 'transaction.execute':
      return {
        base: 'bg-indigo-500',
        hover: 'hover:bg-indigo-600',
        disabled: 'bg-gray-400 text-gray-200 cursor-not-allowed',
      }
    case 'transaction.commit':
      return {
        base: 'bg-emerald-500',
        hover: 'hover:bg-emerald-600',
        disabled: 'bg-gray-400 text-gray-200 cursor-not-allowed',
      }
    case 'transaction.rollback':
      return {
        base: 'bg-rose-500',
        hover: 'hover:bg-rose-600',
        disabled: 'bg-gray-400 text-gray-200 cursor-not-allowed',
      }
    case 'catalog.subscribe':
      return {
        base: 'bg-indigo-500',
        hover: 'hover:bg-indigo-600',
        disabled: 'bg-gray-400 text-gray-200 cursor-not-allowed',
      }
    case 'catalog.unsubscribe':
      return {
        base: 'bg-purple-500',
        hover: 'hover:bg-purple-600',
        disabled: 'bg-gray-400 text-gray-200 cursor-not-allowed',
      }
    case 'catalog.list_tables':
      return {
        base: 'bg-blue-500',
        hover: 'hover:bg-blue-600',
        disabled: 'bg-gray-400 text-gray-200 cursor-not-allowed',
      }
    case 'catalog.load_table':
      return {
        base: 'bg-emerald-500',
        hover: 'hover:bg-emerald-600',
        disabled: 'bg-gray-400 text-gray-200 cursor-not-allowed',
      }
    case 'catalog.drop_table':
      return {
        base: 'bg-red-600',
        hover: 'hover:bg-red-700',
        disabled: 'bg-gray-400 text-gray-200 cursor-not-allowed',
      }
    case 'catalog.list_definitions':
      return {
        base: 'bg-cyan-500',
        hover: 'hover:bg-cyan-600',
        disabled: 'bg-gray-400 text-gray-200 cursor-not-allowed',
      }
    case 'clear':
      return {
        base: 'bg-orange-500',
        hover: 'hover:bg-orange-600',
        disabled: 'bg-gray-400 text-gray-200 cursor-not-allowed',
      }
    default:
      return {
        base: 'bg-gray-500',
        hover: 'hover:bg-gray-600',
        disabled: 'bg-gray-400 text-gray-200 cursor-not-allowed',
      }
  }
}
