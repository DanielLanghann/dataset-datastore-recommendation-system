import React from 'react';

const CustomSwitch = ({
  label,
  id,
  name,
  checked,
  onChange,
  disabled = false,
}) => {
  return (
    <div className="flex items-center space-x-3">
      <label
        htmlFor={id}
        className="text-xl font-medium text-white font-markpro"
      >
        {label}
      </label>
      <div className="relative">
        <input
          type="checkbox"
          id={id}
          name={name}
          checked={checked}
          onChange={onChange}
          disabled={disabled}
          className="sr-only"
        />
        <div
          className={`block w-12 h-6 rounded-full transition-colors duration-300 ease-in-out cursor-pointer ${
            disabled ? 'opacity-50 cursor-not-allowed' : ''
          } ${
            checked ? 'bg-green-500' : 'bg-gray-600'
          }`}
          onClick={() => !disabled && onChange({ target: { name, checked: !checked } })}
        >
          <div
            className={`absolute left-1 top-1 w-4 h-4 bg-white rounded-full transition-transform duration-300 ease-in-out ${
              checked ? 'transform translate-x-6' : 'transform translate-x-0'
            }`}
          />
        </div>
      </div>
    </div>
  );
};

export default CustomSwitch;
