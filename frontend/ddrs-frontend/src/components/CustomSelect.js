import React from 'react';

const CustomSelect = ({
  label,
  id,
  name,
  value,
  onChange,
  options,
  required = false,
  disabled = false,
  placeholder = "Select an option",
}) => {
  return (
    <div className="space-y-2">
      <label
        htmlFor={id}
        className="block text-xl font-medium text-white font-markpro"
      >
        {label}
      </label>
      <select
        id={id}
        name={name}
        value={value}
        onChange={onChange}
        required={required}
        disabled={disabled}
        className="w-full px-4 py-3 bg-gray-700 border border-gray-600 rounded-lg text-white focus:outline-none focus:ring-2 focus:ring-green-400 focus:border-transparent text-lg font-markpro disabled:opacity-50 disabled:cursor-not-allowed"
      >
        <option value="">{placeholder}</option>
        {options.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>
    </div>
  );
};

export default CustomSelect;
