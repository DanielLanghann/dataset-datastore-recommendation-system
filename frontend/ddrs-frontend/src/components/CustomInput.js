import React from 'react';

const CustomInput = ({
  label,
  id,
  type = 'text',
  placeholder,
  value,
  onChange,
  required = false,
  autoFocus = false,
}) => {
  return (
    <div className="space-y-2">
      <label
        htmlFor={id}
        className="block text-xl font-medium text-white font-markpro"
      >
        {label}
      </label>
      <input
        id={id}
        type={type}
        placeholder={placeholder}
        value={value}
        onChange={onChange}
        required={required}
        autoFocus={autoFocus}
        className="w-full px-4 py-3 bg-gray-700 border border-gray-600 rounded-lg text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-green-400 focus:border-transparent text-lg font-markpro"
      />
    </div>
  );
};

export default CustomInput;
