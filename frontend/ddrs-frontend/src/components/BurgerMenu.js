import React, { useState, useRef, useEffect } from 'react';
import { createPortal } from 'react-dom';
import { MoreVertical, Edit, Trash2 } from 'lucide-react';

const BurgerMenu = ({ onUpdate, onDelete }) => {
  const [isOpen, setIsOpen] = useState(false);
  const [menuPosition, setMenuPosition] = useState({ top: 0, left: 0 });
  const menuRef = useRef(null);
  const buttonRef = useRef(null);

  useEffect(() => {
    const handleClickOutside = (event) => {
      // Don't close if clicking on the button itself
      if (buttonRef.current && buttonRef.current.contains(event.target)) {
        return;
      }
      
      // Close if clicking outside the menu
      if (menuRef.current && !menuRef.current.contains(event.target)) {
        setIsOpen(false);
      }
    };

    const handleEscape = (event) => {
      if (event.key === 'Escape' && isOpen) {
        setIsOpen(false);
      }
    };

    if (isOpen) {
      document.addEventListener('mousedown', handleClickOutside);
      document.addEventListener('keydown', handleEscape);
    }
    
    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
      document.removeEventListener('keydown', handleEscape);
    };
  }, [isOpen]);

  const updateMenuPosition = () => {
    if (buttonRef.current) {
      const buttonRect = buttonRef.current.getBoundingClientRect();
      const menuWidth = 160;
      const menuHeight = 80;
      const padding = 8;
      
      // Simple positioning: below and to the left of the button
      let left = buttonRect.right - menuWidth;
      let top = buttonRect.bottom + padding;
      
      // Ensure menu doesn't go off the right edge
      if (left < padding) {
        left = buttonRect.left;
      }
      
      // If menu would go off the right edge, position it to the left
      if (left + menuWidth > window.innerWidth - padding) {
        left = buttonRect.left - menuWidth - padding;
      }
      
      // Ensure menu doesn't go off the bottom
      if (top + menuHeight > window.innerHeight - padding) {
        top = buttonRect.top - menuHeight - padding;
      }
      
      // Ensure menu doesn't go off the top
      if (top < padding) {
        top = buttonRect.bottom + padding;
      }
      
      setMenuPosition({ top, left });
    }
  };

  useEffect(() => {
    if (isOpen) {
      // Use requestAnimationFrame to ensure DOM is updated
      requestAnimationFrame(() => {
        updateMenuPosition();
      });
    }
  }, [isOpen]);

  const handleToggle = () => {
    setIsOpen(!isOpen);
  };

  const handleUpdate = () => {
    setIsOpen(false);
    onUpdate();
  };

  const handleDelete = () => {
    setIsOpen(false);
    onDelete();
  };

  const menuElement = isOpen && (
    <div 
      ref={menuRef}
      className="fixed w-40 bg-gray-800 border border-gray-600 rounded-md shadow-xl transform transition-opacity duration-150 ease-out"
      style={{
        top: `${menuPosition.top}px`,
        left: `${menuPosition.left}px`,
        zIndex: 9999,
        minWidth: '140px',
        maxWidth: 'calc(100vw - 16px)',
      }}
    >
      <div className="py-1">
        <button
          onClick={handleUpdate}
          className="flex items-center w-full px-3 py-2 text-sm text-gray-200 hover:bg-gray-700 transition-colors duration-200 text-left"
        >
          <Edit className="w-4 h-4 mr-2 flex-shrink-0" />
          <span className="truncate">Update</span>
        </button>
        <button
          onClick={handleDelete}
          className="flex items-center w-full px-3 py-2 text-sm text-red-400 hover:bg-gray-700 transition-colors duration-200 text-left"
        >
          <Trash2 className="w-4 h-4 mr-2 flex-shrink-0" />
          <span className="truncate">Delete</span>
        </button>
      </div>
    </div>
  );

  return (
    <>
      <div className="flex justify-center items-center">
        <button
          ref={buttonRef}
          onClick={handleToggle}
          className="p-2 min-w-[32px] min-h-[32px] rounded-full hover:bg-gray-700 transition-colors duration-200 flex items-center justify-center touch-manipulation focus:outline-none focus:ring-2 focus:ring-gray-500"
          aria-label="More actions"
          aria-haspopup="true"
          aria-expanded={isOpen}
        >
          <MoreVertical className="w-4 h-4 text-gray-300" />
        </button>
      </div>
      {menuElement && createPortal(menuElement, document.body)}
    </>
  );
};

export default BurgerMenu;
