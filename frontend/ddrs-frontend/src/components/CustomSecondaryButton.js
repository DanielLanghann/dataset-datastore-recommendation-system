import React from "react";

const CustomSecondaryButton = ({
  children,
  onClick,
  type = "button",
  disabled = false,
  size = "md",
  textSize = "text-lg",
  fullWidth = false,
  color = "blue",
}) => {
  const sizeClasses = {
    sm: "h-10 px-4",
    md: "h-12 px-6",
    lg: "h-14 px-8",
    xl: "h-16 px-10",
  };

  const colorClasses = {
    blue: {
      bg: "bg-blue-500 hover:bg-blue-600",
      ring: "focus:ring-blue-400",
    },
    gray: {
      bg: "bg-gray-500 hover:bg-gray-600",
      ring: "focus:ring-gray-400",
    },
  };

  const currentColor = colorClasses[color] || colorClasses.blue;

  return (
    <button
      className={`
        ${currentColor.bg} text-white font-bold rounded-lg
        transition duration-300 font-markpro focus:outline-none focus:ring-2
        ${currentColor.ring} focus:ring-opacity-50 ${textSize}
        ${disabled ? "opacity-50 cursor-not-allowed" : ""}
        flex items-center justify-center
        ${sizeClasses[size] || sizeClasses.md}
        ${fullWidth ? "w-full" : ""}
      `}
      onClick={onClick}
      type={type}
      disabled={disabled}
    >
      {children}
    </button>
  );
};

export default CustomSecondaryButton;
