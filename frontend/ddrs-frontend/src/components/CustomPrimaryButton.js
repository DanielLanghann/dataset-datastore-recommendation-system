import React from "react";

const CustomPrimaryButton = ({
  children,
  onClick,
  type = "button",
  disabled = false,
  size = "md",
  textSize = "text-2xl",
  fullWidth = false,
}) => {
  const sizeClasses = {
    sm: "h-10 px-4",
    md: "h-12 px-6",
    lg: "h-14 px-8",
    xl: "h-16 px-10",
  };

  return (
    <button
      className={`
        bg-green-500 hover:bg-green-600 text-white font-bold rounded-full
        transition duration-300 font-markpro focus:outline-none focus:ring-2
        focus:ring-green-400 focus:ring-opacity-50 ${textSize}
        ${disabled ? "opacity-50 cursor-not-allowed" : ""}
        flex items-center justify-center
        ${sizeClasses[size] || sizeClasses.md}
        ${fullWidth ? "w-full" : ""}
      `}
      onClick={onClick}
      type={type}
      disabled={disabled}
      style={{
        background: disabled
          ? "gray"
          : "linear-gradient(to right, #6ec189, #3b9473)",
      }}
    >
      {children}
    </button>
  );
};

export default CustomPrimaryButton;
